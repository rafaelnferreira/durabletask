namespace DurableTask.AzureStorage.Partitioning
{
    using Azure.Data.Tables;
    using Azure;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Text.Json;
    using System.Collections.Concurrent;

    /// <summary>
    /// Partition ManagerV3 is based on the Table storage.  
    /// </summary>
    public class TablePartitionManager
    {
        /// <summary>
        /// This represents the Table which the manager operates on. 
        /// </summary>
        public TableClient partitionTable;
        
        /// <summary>
        /// Name of the tableLeaseManager. 
        /// </summary>
        public string workerName;
        
        /// <summary>
        /// The worker which will read and write on the table.
        /// </summary>
        TableLeaseManager tableLeaseManager;

        /// <summary>
        /// Token that governs the initiation and termination of the worker
        /// </summary>
        public CancellationTokenSource source = new CancellationTokenSource();
        CancellationToken token;
        
        /// <summary>
        /// This method create a new instance of the class TableLeaseManager that represents the worker. 
        /// And then start the loop that the worker keeps operating on the table. 
        /// </summary>
        /// <returns></returns>
        public void StartAsync()
        {
            this.tableLeaseManager = new TableLeaseManager { myTable = this.partitionTable, workerName = this.workerName };
            token = source.Token;

            _ = Task.Run(() => this.PartitionManagerLoop(token));
            Trace.TraceInformation(DateTime.UtcNow.ToString() + " Worker " + this.workerName + " starts working.");
        }

        async Task PartitionManagerLoop(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                int timeToSleep = 15000;
                try
                {
                    var response = await this.tableLeaseManager.ReadAndWriteTable();
                    if (response.workOnRelease || response.waitForPartition)
                    {
                        timeToSleep = 1000;
                    }
                }
                catch
                {
                    timeToSleep = 0;
                }

                await Task.Delay(timeToSleep, token);
            }
        }

        /// <summary>
        /// This method will stop the worker. It first stops the task ReadAndWriteTable().
        /// And then start the Task ShutDown() until all the leases in the worker is drained. 
        /// </summary>
        /// <returns></returns>
        public async Task StopAsync()
        {
            this.source.Cancel();
            bool shouldRetry = false;
            int timeToSleep = 1000;
            Trace.TraceInformation(DateTime.UtcNow.ToString() + " Worker " + this.workerName + " begins to shut down.");
            var task = Task.Run(async () =>
            {
                while (!shouldRetry)
                {
                    try
                    {
                        shouldRetry = await this.tableLeaseManager.ShutDown();
                    }
                    catch
                    {
                        timeToSleep = 0;
                    }
                    await Task.Delay(timeToSleep);
                }
            });
            await task;
            Trace.TraceInformation(DateTime.UtcNow.ToString() + " Worker " + this.workerName + " finishes shutting down.");
        }

        class TableLeaseManager
        {
            public string workerName { get; set; }
            public TableClient myTable { get; set; }

            public Dictionary<string, Task> tasks = new Dictionary<string, Task>(); 

            public async Task<ReadTableReponse> ReadAndWriteTable()
            {
                var response = new ReadTableReponse();
                Pageable<TableLease> partitions = myTable.Query<TableLease>();
                Dictionary<string, List<TableLease>> partitionDistribution = new Dictionary<string, List<TableLease>>(); 
                int leaseNum = 0;

                foreach (var partition in partitions)
                {
                    bool isClaimedLease = false;
                    bool isStealedLease = false;
                    bool isRenewdLease = false;
                    bool isDrainedLease = false;
                    bool isReleasedLease = false;

                    bool isEmptyLease = (partition.CurrentOwner == null && partition.NextOwner == null);
                    bool isExpiresAt = (DateTime.UtcNow >= partition.ExpiresAt && partition.NextOwner == null);
                    bool isStolen = (partition.CurrentOwner == null && partition.NextOwner == this.workerName);

                    var etag = partition.ETag;

                    if (isEmptyLease || isExpiresAt || isStolen)
                    {
                        ClaimLease(partition);
                        isClaimedLease = true;

                    }

                    bool isOtherWorkerCurrentLease = (partition.CurrentOwner != workerName && partition.NextOwner == null && partition.IsDraining == false);
                    bool isOtherWorkerFutureLease = (partition.CurrentOwner != workerName && partition.NextOwner != null);
                    bool isOtherWorkerShutDownLease = (partition.CurrentOwner != workerName && partition.NextOwner == null && partition.IsDraining == true);

                    if (isOtherWorkerCurrentLease)
                    {
                        if (partitionDistribution.ContainsKey(partition.CurrentOwner))
                        {
                            partitionDistribution[partition.CurrentOwner].Add(partition);
                        }
                        else
                        {
                            partitionDistribution.Add(partition.CurrentOwner, new List<TableLease> { partition });
                        }
                    }

                    if (isOtherWorkerFutureLease)
                    {
                        if (partitionDistribution.ContainsKey(partition.NextOwner))
                        {
                            partitionDistribution[partition.NextOwner].Add(partition);
                        }
                        else
                        {
                            partitionDistribution.Add(partition.NextOwner, new List<TableLease> { partition });
                        }
                        if (partition.NextOwner == workerName)
                        {
                            leaseNum++;
                            response.waitForPartition = true;
                        }
                    }

                    if (isOtherWorkerShutDownLease)
                    {
                        StealLease(partition);
                        isStealedLease = true;
                        response.waitForPartition = true;
                    }

                    if (partition.CurrentOwner == workerName)
                    {
                        if (partition.NextOwner == null)
                        {
                            leaseNum++;
                        }
                        else
                        {
                            response.workOnRelease = true;

                            if (partition.IsDraining == false)
                            {
                                DrainLease(partition);
                                isDrainedLease = true;
                            }
                            else
                            {
                                if (tasks[partition.RowKey].IsCompleted == true)
                                {
                                    ReleaseLease(partition);
                                    isReleasedLease = true;
                                }
                            }
                        }
                        if (partition.CurrentOwner != null)
                        {
                            RenewLease(partition);
                            isRenewdLease = true;
                        }
                    }

                    if (isClaimedLease || isStealedLease || isRenewdLease || isDrainedLease || isReleasedLease)
                    {
                        try
                        {
                            await myTable.UpdateEntityAsync(partition, etag, (TableUpdateMode)1);
                            if(isClaimedLease)
                            {
                                Trace.TraceInformation(DateTime.UtcNow.ToString() + " Worker " + this.workerName + " starts listening to " + partition.RowKey);
                            }
                            if (isStealedLease)
                            {
                                Trace.TraceInformation(DateTime.UtcNow.ToString() + " Worker " + this.workerName + " steals the lease of" + partition.RowKey);
                            }
                            if (isReleasedLease)
                            {
                                Trace.TraceInformation(DateTime.UtcNow.ToString() + " Worker " + this.workerName + " stops listening to " + partition.RowKey);
                            }
                            if(isDrainedLease)
                            {
                                Trace.TraceInformation(DateTime.UtcNow.ToString() + " Worker " + this.workerName + " starts draining the lease of " + partition.RowKey);
                            }
                        }
                        catch (RequestFailedException ex) when (ex.Status == 412)
                        {
                            throw ex;
                        }
                    }
                }

                if (partitionDistribution.Count != 0)
                {
                    int numLeasePerWorkerForBalance = (partitions.Count()) / (partitionDistribution.Count + 1);
                    if (numLeasePerWorkerForBalance == 0) numLeasePerWorkerForBalance = 1;
                    int numOfLeaseToSteal = numLeasePerWorkerForBalance - leaseNum;

                    while (numOfLeaseToSteal > 0)
                    {
                        int n = 0;
                        foreach (KeyValuePair<string, List<TableLease>> pair in partitionDistribution)
                        {
                            n++;
                            int currentWorkerNumofLeases = pair.Value.Count;
                            if (currentWorkerNumofLeases > numLeasePerWorkerForBalance)
                            {
                                foreach (TableLease partition in pair.Value)
                                {
                                    {
                                        numOfLeaseToSteal--;
                                        currentWorkerNumofLeases--;
                                        var etag = partition.ETag;
                                        StealLease(partition);
                                        try
                                        {
                                            await myTable.UpdateEntityAsync(partition, etag, (TableUpdateMode)1);
                                            Trace.TraceInformation(DateTime.UtcNow.ToString() + " Worker " + this.workerName + " steals the lease of" + partition.RowKey);
                                            response.waitForPartition = true;
                                        }
                                        catch (RequestFailedException ex) when (ex.Status == 412)
                                        {
                                            throw ex;
                                        }
                                    }

                                    if (currentWorkerNumofLeases == numLeasePerWorkerForBalance || numOfLeaseToSteal == 0) { break; }
                                }
                            }
                            if (numOfLeaseToSteal == 0) { break; }
                        }
                        if (n == partitionDistribution.Count) { break; }
                    }
                }
                return response;
            }


            public void ClaimLease(TableLease partition)
            {
                partition.CurrentOwner = workerName;
                partition.NextOwner = null;
                partition.OwnedSince = DateTime.UtcNow;
                partition.LastRenewal = DateTime.UtcNow;
                partition.ExpiresAt = DateTime.UtcNow.AddMinutes(1);
                partition.IsDraining = false;
            }
            public void DrainLease(TableLease partition)
            {
                partition.IsDraining = true;

                var task = Task.Run(() =>
                {
                    Thread.CurrentThread.Name = partition.RowKey;
                    Thread.Sleep(10000);
                }
                );
                if (tasks.ContainsKey(partition.RowKey))
                {
                    tasks[partition.RowKey] = task;
                }
                else
                {
                    tasks.Add(partition.RowKey, task);
                }
            }

            public void ReleaseLease(TableLease partition)
            {
                partition.IsDraining = false;
                partition.CurrentOwner = null;
            }
            public void RenewLease(TableLease partition) 
            {
                partition.ExpiresAt = DateTime.UtcNow.AddMinutes(1);
            }


            public void StealLease(TableLease partition)
            {
                partition.NextOwner = workerName;
            }

            public async Task<bool> ShutDown()
            {
                Pageable<TableLease> partitions = myTable.Query<TableLease>();
                int leaseNum = 0;
                foreach (var partition in partitions)
                {
                    bool isDrainedLease = false;
                    bool isReleasedLease = false;

                    if (partition.CurrentOwner == workerName)
                    {
                        leaseNum++;
                        var etag = partition.ETag;
                        
                        if (partition.IsDraining == false)
                        {
                            DrainLease(partition);
                            RenewLease(partition);
                            isDrainedLease = true;
                        }
                        else
                        {
                            if (tasks[partition.RowKey].IsCompleted == true)
                            {
                                ReleaseLease(partition);
                                isReleasedLease = true;
                                leaseNum--;
                            }
                            else
                            {
                                RenewLease(partition);
                            }
                        }
                        
                        try
                        {
                            await myTable.UpdateEntityAsync(partition, etag, (TableUpdateMode)1);
                            if (isDrainedLease)
                            {
                                Trace.TraceInformation(DateTime.UtcNow.ToString() + " Worker " + this.workerName + " starts draining the lease of " + partition.RowKey);
                            }
                            if (isReleasedLease)
                            {
                                Trace.TraceInformation(DateTime.UtcNow.ToString() + " Worker " + this.workerName + " stops listening to " + partition.RowKey);
                            }
                            
                        }
                        catch (RequestFailedException ex) when (ex.Status == 412)
                        {
                            throw ex;
                        }
                    }

                }
                if (leaseNum == 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }

            }
        }

        /// <summary>
        ///The Response class describes the behavior of the ReadandWrite method in the PartitionManager worker class. 
        ///If the virtual machine is about to be drained, the method sets the WorkonRelease flag to true. 
        ///If the VM is going to acquire another lease, it sets the waitforPartition flag to true. 
        ///When either of these flags is true, the sleep time of the VM changes from 15 seconds to 1 second.
        /// </summary>
        class ReadTableReponse
        {
            public bool workOnRelease { get; set; } = false;
            public bool waitForPartition { get; set; } = false;
        }
    }

    /// <summary>
    /// This class defines the lease that will be saved in the Table storage.
    /// </summary>
    public class TableLease : ITableEntity
    {
        /// <summary>
        /// Empty string. Not used for now.
        /// </summary>
        public string PartitionKey { get; set; } = null;

        /// <summary>
        /// The name of the partition/control queue.
        /// </summary>
        public string RowKey { get; set; }

        /// <summary>
        /// The current owner name for this lease. 
        /// </summary>
        public string CurrentOwner { get; set; }

        /// <summary>
        /// The name of the worker that is stealing, or null if nobody is trying to steal it.
        /// </summary>
        public string NextOwner { get; set; }
        
        /// <summary>
        /// The timestamp at which the partition was originally acquired by this worker. 
        /// </summary>
        public DateTime? OwnedSince { get; set; }

        /// <summary>
        /// The timestamp at which the partition was last renewed.
        /// </summary>
        public DateTime? LastRenewal { get; set; }

        /// <summary>
        /// The timestamp at which the partition lease expires.
        /// </summary>
        public DateTime? ExpiresAt { get; set; }

        /// <summary>
        /// True if the partition is being drained; False otherwise.
        /// </summary>
        public bool IsDraining { get; set; } = false;
        
        /// <summary>
        /// Required atrribute of Azure.Data.Tables storage entity. Not used. 
        /// </summary>
        public DateTimeOffset? Timestamp { get; set; }

        /// <summary>
        /// Unique identifier used to version entities and ensure concurrency safety in Azure.Data.Tables.
        /// </summary>
        public ETag ETag { get; set; }
    }
}
