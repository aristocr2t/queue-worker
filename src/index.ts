import amqp from 'amqplib';
import { EventEmitter } from 'events';
import ms from 'ms';
import { Rabbit } from 'rabbit-queue';
import { isArray, isNumber, isObject } from 'util';

export interface SingleWorkerOptions {
  attemptsCount?: number;
  attemptDelays?: string[];
}

export interface WorkerOptions extends SingleWorkerOptions {
  jobsCount?: number;
}

export type SuccessCallback<T, R = any> = (message: T, result: R) => any;
export type FailCallback<T, E = Error> = (message: T, error: E) => any;
export type ErrorCallback<T, E = Error> = (message: T, errors: E[]) => any;

type WorkerMessage<T, E = Error> = {
  jobIndex: number;
  attempt: number;
  attemptsCount: number;
  attemptDelays: string[];
  content: T;
  errors: E[];
};

// tslint:disable-next-line:max-line-length
const MS_MASK = /^((?:\d+)?\.?\d+) *(milliseconds?|msecs?|ms|seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d|weeks?|w|years?|yrs?|y)?$/i;

export class QueueWorker<T, E = Error, R = any> {
  static readonly DEFAULT_OPTIONS = {
    jobsCount: 1,
    attemptsCount: 5,
    attemptDelays: ['0', '10s', '1m'],
  };

  static ERROR_HANDLER: (message: any, ...optionalParams: any[]) => any = console.error;

  private readonly rabbit: Rabbit;
  private readonly queue: string;
  private readonly options: WorkerOptions;

  private readonly successCallbacks: Array<SuccessCallback<T, R>> = [];
  private readonly failCallbacks: Array<FailCallback<T, E>> = [];
  private readonly errorCallbacks: Array<ErrorCallback<T, E>> = [];
  private readonly activeJobs: number[] = [];

  static setDefaultOptions(options: WorkerOptions): void {
    if (isNumber(options.jobsCount)) this.DEFAULT_OPTIONS.jobsCount = options.jobsCount;
    if (isNumber(options.attemptsCount)) this.DEFAULT_OPTIONS.attemptsCount = options.attemptsCount;
    this.DEFAULT_OPTIONS.attemptDelays = this.parseAttemptDelays(options.attemptDelays);
  }

  private static parseAttemptDelays(attemptDelays: string[]): string[] {
    return (isArray(attemptDelays) ? attemptDelays : QueueWorker.DEFAULT_OPTIONS.attemptDelays).filter(v => MS_MASK.test(v));
  }

  private static errorHandler(err: any): void {
    if (this.ERROR_HANDLER instanceof Function) {
      this.ERROR_HANDLER(err);
    }
  }

  constructor(rabbit: Rabbit, queue: string, options?: WorkerOptions) {
    if (!(rabbit instanceof EventEmitter)) {
      throw new Error('Rabbit must be instance of EventEmitter');
    }
    if (!(queue && typeof queue === 'string')) {
      throw new Error('Queue must be a non-empty string');
    }
    this.rabbit = rabbit;
    this.queue = queue;
    this.options = QueueWorker.DEFAULT_OPTIONS;
    if (isObject(options)) {
      if (isNumber(options.jobsCount)) this.options.jobsCount = options.jobsCount;
      if (isNumber(options.attemptsCount)) this.options.attemptsCount = options.attemptsCount;
      this.options.attemptDelays = QueueWorker.parseAttemptDelays(this.options.attemptDelays);
    }
    this.activeJobs = Array(this.options.jobsCount).fill(0);
    Promise.all(this.activeJobs.map((_, i) => this.rabbit.createQueue(this.queueName(i), { durable: true }))).catch(err =>
      QueueWorker.errorHandler(err),
    );
  }

  private delay(...args: any[]): Promise<any[]> {
    return new Promise<any[]>(resolve => setTimeout(() => resolve(args)));
  }

  addItem(data: T, options?: SingleWorkerOptions): void {
    this.delay(options)
      .then(([options]) => {
        options = typeof options === 'object' && options !== null ? options : this.options;
        const jobIndex = this.getFreeJobIndex();
        const attempt = 0;
        const attemptsCount = isNumber(options.attemptsCount) ? options.attemptsCount : this.options.attemptsCount;
        const attemptDelays = QueueWorker.parseAttemptDelays(options.attemptDelays);
        return this.send(
          jobIndex,
          {
            jobIndex,
            attempt,
            attemptsCount,
            attemptDelays,
            content: data,
            errors: [],
          },
          { expiration: ms(attemptDelays[attempt] || '0s') },
        );
      })
      .catch(err => QueueWorker.errorHandler(err));
  }

  private getFreeJobIndex(): number {
    let index: number;
    let min = Infinity;
    for (let i = 0; i < this.activeJobs.length; i++) {
      if (this.activeJobs[i] < min) {
        min = this.activeJobs[i];
        index = i;
      }
    }
    this.activeJobs[index]++;
    return index;
  }

  private freeJob(index: number): void {
    if (this.activeJobs[index] > 0) {
      this.activeJobs[index]--;
    }
  }

  private queueName(index: number): string {
    return `${this.queue}-${index}`;
  }

  on(type: 'success', callbackFn: SuccessCallback<T, R>): void;
  on(type: 'fail', callbackFn: FailCallback<T, E>): void;
  on(type: 'error', callbackFn: ErrorCallback<T, E>): void;
  on(type: 'success' | 'fail' | 'error', callbackFn: SuccessCallback<T, R> | FailCallback<T, E> | ErrorCallback<T, E>): void {
    if (callbackFn instanceof Function) {
      if (type === 'success') {
        this.successCallbacks.push(callbackFn as SuccessCallback<T, R>);
      } else if (type === 'fail') {
        this.failCallbacks.push(callbackFn as FailCallback<T, E>);
      } else if (type === 'error') {
        this.errorCallbacks.push(callbackFn as ErrorCallback<T, E>);
      }
    }
  }

  private rabbitHandler(callbackFn: (data: T) => R | PromiseLike<R>): (msg: amqp.Message, ack: (reply: any) => any) => Promise<void> {
    return async (msg: amqp.Message, ack: (reply: any) => any) => {
      const message = JSON.parse(msg.content.toString()) as WorkerMessage<T, E>;
      try {
        let value = callbackFn(message.content);
        if (value instanceof Promise) value = await value;
        this.successCallbacks.map(this.callbackHandler(message, value as R));
        this.freeJob(message.jobIndex);
      } catch (err) {
        this.freeJob(message.jobIndex);
        this.failCallbacks.map(this.callbackHandler(message, err));
        message.errors.push(err.message || err);
        this.retryMessage(message);
      }
      ack(msg);
    };
  }

  handle(callbackFn: (data: T) => R | PromiseLike<R>): void {
    this.delay(callbackFn)
      .then(async ([callbackFn]) => {
        for (let i = 0; i < this.options.jobsCount; i++) {
          await this.rabbit.subscribe(this.queueName(i), this.rabbitHandler(callbackFn));
        }
      })
      .catch(err => QueueWorker.errorHandler(err));
  }

  private callbackHandler(
    message: WorkerMessage<T, E>,
    result: R | E | E[],
  ): (cb: SuccessCallback<T, R> | FailCallback<T, E> | ErrorCallback<T, E>) => void {
    return (cb: (message: T, result: R | E | E[]) => any) => {
      this.delay(message, result)
        .then(async ([message, result]: [WorkerMessage<T, E>, R | E | E[]]) => {
          let value = cb(message.content, result);
          if (value instanceof Promise) {
            value = await value;
          }
        })
        .catch(err => QueueWorker.errorHandler(err));
    };
  }

  private async send(index: number, message: WorkerMessage<T, E>, options?: amqp.Options.Publish): Promise<void> {
    if (!options) options = {};
    const args = [
      this.queueName(index),
      message,
      {
        ...options,
        contentEncoding: 'utf8',
        contentType: 'application/json',
        persistent: true,
      },
    ];
    if (options.expiration) {
      await this.rabbit.publishWithDelay.apply(this.rabbit, args);
    } else {
      await this.rabbit.publish.apply(this.rabbit, args);
    }
  }

  private retryMessage(message: WorkerMessage<T, E>): void {
    this.delay(message).then(async ([message]: [WorkerMessage<T, E>]) => {
      message.attempt++;
      if (message.attempt <= message.attemptsCount) {
        const delay = message.attemptDelays[message.attempt] || message.attemptDelays[message.attemptDelays.length - 1];
        const i = this.getFreeJobIndex();
        message.jobIndex = i;
        try {
          await this.send(i, message, { expiration: ms(delay) });
        } catch (err) {
          QueueWorker.errorHandler(err);
        }
      } else {
        this.errorCallbacks.map(this.callbackHandler(message, message.errors));
      }
    });
  }
}
