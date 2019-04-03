import * as amqp from 'amqplib';
import { EventEmitter } from 'events';
import * as ms from 'ms';
import { Rabbit } from 'rabbit-queue';

export interface WorkerOptions {
  jobsCount?: number;
  attemptCount?: number;
  attemptDelays?: string[];
}

export type WorkerMessage<T> = {
  jobIndex: number;
  attempt: number;
  attemptCount: number;
  attemptDelays: string[];
  content: T;
  errors: (Error | any)[];
};

// tslint:disable-next-line:max-line-length
const MS_MASK = /^((?:\d+)?\.?\d+) *(milliseconds?|msecs?|ms|seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d|weeks?|w|years?|yrs?|y)?$/i;

export class QueueWorker<T> {
  static DEFAULT_OPTIONS = {
    jobsCount: 1,
    attemptCount: 5,
    attemptDelays: ['0s', '10s', '1m']
  };

  static ERROR_HANDLER: Function = console.error;

  private rabbit: Rabbit;
  private queue: string;
  private options: WorkerOptions = QueueWorker.DEFAULT_OPTIONS;

  private errorCallbacks: Function[] = [];
  private failCallbacks: Function[] = [];
  private successCallbacks: Function[] = [];
  private activeJobs: number[] = [];

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
    if (typeof options === 'object' && options !== null) this.options = Object.assign({}, this.options, options);
    this.options.attemptDelays = (this.options.attemptDelays instanceof Array
      ? this.options.attemptDelays
      : QueueWorker.DEFAULT_OPTIONS.attemptDelays
    ).filter(v => MS_MASK.test(v));
    this.activeJobs = Array(this.options.jobsCount).fill(0);
    this.activeJobs.forEach((c, i) => {
      this.rabbit.createQueue(this.queueName(i), { durable: true }).then();
    });
  }

  addItem(data: T, options?: WorkerOptions): void {
    options = typeof options === 'object' && options !== null ? options : this.options;
    const jobIndex = this.getFreeJobIndex(true);
    const attempt = 0;
    const attemptCount = options.attemptCount;
    const attemptDelays = (options.attemptDelays instanceof Array
      ? options.attemptDelays
      : QueueWorker.DEFAULT_OPTIONS.attemptDelays
    ).filter(v => MS_MASK.test(v));
    this.send(
      jobIndex,
      { jobIndex, attempt, attemptCount, attemptDelays, content: data, errors: [] },
      { expiration: ms(attemptDelays[attempt] || '0s') }
    ).catch(err => QueueWorker.errorHandler(err));
  }

  private getFreeJobIndex(setAsBusy: boolean = false): number {
    let index: number,
      min = Infinity;
    for (let i = 0; i < this.activeJobs.length; i++) {
      if (this.activeJobs[i] < min) {
        min = this.activeJobs[i];
        index = i;
      }
    }
    if (setAsBusy) this.activeJobs[index]++;
    return index;
  }

  private queueName(index: number): string {
    return `${this.queue}-${index}`;
  }

  private async send(index: number, message: WorkerMessage<T>, options?: amqp.Options.Publish): Promise<void> {
    if (!options) options = {};
    const args = [
      this.queueName(index),
      message,
      { ...options, contentEncoding: 'utf8', contentType: 'application/json', persistent: true }
    ];
    if (options.expiration) {
      await this.rabbit.publishWithDelay.apply(this.rabbit, args);
    } else {
      await this.rabbit.publish.apply(this.rabbit, args);
    }
  }

  on(type: 'success' | 'fail' | 'error', callbackFn: Function): void {
    if (callbackFn instanceof Function) {
      if (type === 'success') {
        this.successCallbacks.push(callbackFn);
      } else if (type === 'fail') {
        this.failCallbacks.push(callbackFn);
      } else if (type === 'error') {
        this.errorCallbacks.push(callbackFn);
      }
    }
  }

  handle(callbackFn: (data: T) => any): void {
    const handler = async (msg: amqp.Message, ack: (reply: any) => any) => {
      const message = JSON.parse(msg.content.toString()) as WorkerMessage<T>;
      try {
        let value = callbackFn(message.content);
        if (value instanceof Promise) value = await value;
        this.successCallbacks.map(this.callbackHandler(message, value));
        this.freeJob(message.jobIndex);
      } catch (err) {
        this.freeJob(message.jobIndex);
        this.failCallbacks.map(this.callbackHandler(message, err));
        message.errors.push(err.message || err);
        this.retryMessage(message);
      }
      ack(msg);
    };
    (async (handler: (msg: any, ack: (reply: any) => any) => Promise<void>) => {
      for (let i = 0; i < this.options.jobsCount; i++) {
        await this.rabbit.subscribe(this.queueName(i), handler);
      }
    })(handler).catch(err => QueueWorker.errorHandler(err));
  }

  private freeJob(index: number): void {
    if (this.activeJobs[index] > 0) {
      this.activeJobs[index]--;
    }
  }

  private callbackHandler(message: WorkerMessage<T>, result?: any): (cb: Function, i: number) => void {
    return (cb: Function) => {
      (async (message: WorkerMessage<T>, result?: any) => {
        let value = cb(this.queueName(message.jobIndex), message.content, result);
        if (value instanceof Promise) {
          value = await value;
        }
      })(message, result).catch(err => QueueWorker.errorHandler(err));
    };
  }

  private retryMessage(message: WorkerMessage<T>): void {
    message.attempt++;
    if (message.attempt <= message.attemptCount) {
      const delay = message.attemptDelays[message.attempt] || message.attemptDelays[message.attemptDelays.length - 1];
      const i = this.getFreeJobIndex(true);
      message.jobIndex = i;
      this.send(i, message, { expiration: ms(delay) }).catch(err => QueueWorker.errorHandler(err));
    } else {
      this.errorCallbacks.map(this.callbackHandler(message, message.errors));
    }
  }
}
