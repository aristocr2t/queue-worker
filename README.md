# queue-worker

RabbitMQ queue worker.

## How to use?

1. To install package run the command `npm install rabbit-queue queue-worker --save`
2. `Rabbit` should be initialized with flag `scheduledPublish: true`:

```typescript
const rabbit = new Rabbit(URL, { scheduledPublish: true });

const worker = new QueueWorker<{ message: string }>(rabbit, 'test', {
  jobsCount: 5,
  attemptCount: 10,
  attemptDelays: ['1 min', '2 min', '5 min']
});

worker.handle(async ({ message }) => {
  // handle data
  console.log(`Hello ${message}`);
});

// on success
worker.on('success', (queueName: string, data: { message: string }, result: any) => {});

// on attempt fail
worker.on('fail', (queueName: string, data: { message: string }, error: any) => {
  // watch me die
});

// on ended attempts
worker.on('error', (queueName: string, data: { message: string }, errors: any[]) => {});

worker.addItem({ message: 'world' });
```

## Main features

1. Simple queue setup.
2. Jobs (`jobsCount`).
3. Attempts (`attemptCount`).
4. Function of repeat on fail after some time (`attemptDelays[attemptNumber]`.
   If length of `attemptDelays` is <= current number of attempt then it uses last element of `attemptDelays`).

## License

Licensed under MIT license
