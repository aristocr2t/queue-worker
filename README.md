# queue-worker

RabbitMQ queue worker.

## How to use?

1. To install package run the command `npm install rabbit-queue queue-worker --save`
2. `Rabbit` should be initialized with flag `scheduledPublish: true`:

```typescript
const rabbit = new Rabbit(URL, { scheduledPublish: true });

export type Message = { message: string };

const worker = new QueueWorker<Message>(rabbit, 'test', {
  jobsCount: 5,
  attemptsCount: 10, // set attempts count here
  attemptDelays: ['0', '2 min', '5 min'],
  // 1st attempt's trying to resolve now
  // on fail 1st attempt, 2nd attempt will try to resolve after 2 minutes
  // on fail 2nd attempt, all next attempts will try to resolve every 5 minutes
});

worker.handle(async ({ message }) => {
  // handle data
  console.log(`Hello ${message}`);
});

// on success
worker.on('success', (data: Message, result: any) => {
  // you can watch success result here
});

// on attempt fail
worker.on('fail', (data: Message, error: any) => {
  // watch me die
});

// on attempts end
worker.on('error', (data: Message, errors: any[]) => {
  // at the end of all attempts
});

worker.addItem({ message: 'world' });
// or you can set non-default options for this message
worker.addItem(
  { message: 'world' },
  {
    attemptsCount: 0,
    attemptDelays: ['10 min'],
    // this item will be handled after 10 minutes without attempts
  },
);
```

## Main features

1. Simple queue setup.
2. Jobs (`jobsCount`).
3. Attempts (`attemptsCount`).
4. Function of repeat on fail after some time (`attemptDelays[attemptNumber]`.
   If length of `attemptDelays` is <= current number of attempt then it uses last element of `attemptDelays`).

## License

Licensed under MIT license
