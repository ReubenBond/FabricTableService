namespace FabricTableService.Journal
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;

    public class OperationPump<T>
    {
        private readonly TaskCompletionSource<int> completed = new TaskCompletionSource<int>();

        private readonly CancellationTokenSource cancellation = new CancellationTokenSource();

        private readonly Thread thread;

        private readonly BlockingCollection<WorkItem> workItems = new BlockingCollection<WorkItem>(128);

        public OperationPump()
        {
            this.thread = new Thread(this.Run);
        }

        public Task Completed
        {
            get
            {
                return this.completed.Task;
            }
        }

        public void Start()
        {
            this.cancellation.Token.ThrowIfCancellationRequested();
            this.thread.Start();
        }

        public void Stop()
        {
            this.cancellation.Token.ThrowIfCancellationRequested();
            this.workItems.CompleteAdding();
            this.cancellation.Cancel();
            this.Completed.Wait();
        }

        public Task<T> Invoke(Func<T> action)
        {
            if (action == null)
            {
                throw new ArgumentNullException("action");
            }

            this.cancellation.Token.ThrowIfCancellationRequested();
            var item = new WorkItem { Action = action, Completion = new TaskCompletionSource<T>() };
            this.workItems.Add(item, this.cancellation.Token);
            return item.Completion.Task;
        }

        private void Run()
        {
            try
            {
                while (!this.cancellation.IsCancellationRequested)
                {
                    var workItem = this.workItems.Take(this.cancellation.Token);
                    try
                    {
                        workItem.Completion.TrySetResult(workItem.Action());
                    }
                    catch (Exception exception)
                    {
                        workItem.Completion.TrySetException(exception);
                    }
                }
            }
            finally
            {
                foreach (var workItem in this.workItems)
                {
                    workItem.Completion.TrySetCanceled();
                }

                this.completed.TrySetResult(0);
            }
        }

        private struct WorkItem
        {
            public Func<T> Action { get; set; }

            public TaskCompletionSource<T> Completion { get; set; }
        }
    }
}