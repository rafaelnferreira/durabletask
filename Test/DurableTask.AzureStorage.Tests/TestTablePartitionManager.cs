
namespace DurableTask.AzureStorage.Tests
{
    using Azure;
    using Azure.Data.Tables;
    using DurableTask.AzureStorage.Partitioning;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage.Blob;
    using System.Collections.Generic;
    using System;
    using System.Diagnostics;
    using System.Runtime.CompilerServices;
    using System.Threading.Tasks;

    using System.Threading;
    using System.Linq;

    [TestClass]
    public class testTablePartitionManager
    {
        [TestCategory("DisabledInCI")]
        [TestMethod]
        // Start with one worker and four partitions.
        // Test the worker could claim all the partitions in 5 seconds.
        public async Task TestOneWorkerWithFourPartition()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(5);
            Stopwatch stopwatch = new Stopwatch();
            TableClient testTable = await CreateTestTable("UseDevelopmentStorage = true", "TestOneWorkerWithFourPartitions", 4);
            TablePartitionManager partitionManager = new TablePartitionManager { partitionTable = testTable, workerName = "0" };
            partitionManager.StartAsync();

            bool isAllPartitionClaimed = false;
            stopwatch.Start();
            while (stopwatch.Elapsed < timeout && !isAllPartitionClaimed)
            {
                Pageable<TableLease> partitions = testTable.Query<TableLease>();
                foreach (var partition in partitions)
                {
                    if (partition.CurrentOwner == null) break;
                    Assert.AreEqual("0", partition.CurrentOwner);
                    if (partition.RowKey == "controlqueue-03") isAllPartitionClaimed = true;
                }
            }

            stopwatch.Stop();
            await partitionManager.StopAsync();
            await testTable.DeleteAsync();
        }

        [TestCategory("DisabledInCI")]
        [TestMethod]
        //Starts with two workers and four partitions.
        //Test that one worker can acquire two partitions. 
        //Since two worker can not start at the same time, and one will start earlier than the another one.
        //There would be a steal process, and test that the lease tranfer will take no longer than 30 sec.
        public async Task TestTwoWorkerWithFourPartitions()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(30);
            Stopwatch stopwatch = new Stopwatch();
            TableClient testTable = await CreateTestTable("UseDevelopmentStorage = true", "TestTwoWorkerWithFourPartitions", 4);
            IList<TablePartitionManager> partitionManagerList = new List<TablePartitionManager>();

            int workerNum = 2;
            for (int i = 0; i < workerNum; i++)
            {
                TablePartitionManager partitionManager = new TablePartitionManager { partitionTable = testTable, workerName = i.ToString() };
                partitionManagerList.Add(partitionManager);
                partitionManager.StartAsync();
            }
            stopwatch.Start();
            bool isBalanced = false;

            while (stopwatch.Elapsed < timeout && !isBalanced)
            {
                int worker0PartitionNum = 0;
                int worker1PartitionNum = 0;
                Pageable<TableLease> partitions = testTable.Query<TableLease>();
                foreach (var partition in partitions)
                {
                    if (partition.CurrentOwner == null) break;
                    if (partition.CurrentOwner == "0") worker0PartitionNum++;
                    if (partition.CurrentOwner == "1") worker1PartitionNum++;
                    if (worker0PartitionNum == worker1PartitionNum)
                    {
                        isBalanced = true;
                        Assert.AreEqual(2, worker1PartitionNum);
                        Assert.AreEqual(2, worker0PartitionNum);
                    }
                }
            }
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.Elapsed <= timeout);
            Assert.IsTrue(isBalanced);

            var tasks = partitionManagerList.Select(partitionManager => partitionManager.StopAsync());
            await Task.WhenAll(tasks);
            await testTable.DeleteAsync();
        }

        [TestCategory("DisabledInCI")]
        [TestMethod]
        //Starts with four workers and four partitions.
        //Test that each worker can acquire four partitions. 
        //Since workers can not start at the same time, there would be a steal lease process.
        //Test that the lease tranfer will take no longer than 30 sec.
        public async Task TestFourWorkerWithFourPartitions()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(40);
            Stopwatch stopwatch = new Stopwatch();
            TableClient testTable = await CreateTestTable("UseDevelopmentStorage = true", "TestFourWorkerWithFourPartitions", 4);
            IList<TablePartitionManager> partitionManagerList = new List<TablePartitionManager>();

            int workerNum = 4;
            for (int i = 0; i < workerNum; i++)
            {
                TablePartitionManager partitionManager = new TablePartitionManager { partitionTable = testTable, workerName = i.ToString() };
                partitionManagerList.Add(partitionManager);
                partitionManager.StartAsync();
            }
            stopwatch.Start();
            bool isBalanced = false;

            while (stopwatch.Elapsed < timeout && !isBalanced)
            {
                int worker0PartitionNum = 0;
                int worker1PartitionNum = 0;
                int worker2PartitionNum = 0;
                int worker3PartitionNum = 0;

                Pageable<TableLease> partitions = testTable.Query<TableLease>();
                foreach (var partition in partitions)
                {
                    if (partition.CurrentOwner == null) break;
                    if (partition.CurrentOwner == "0") worker0PartitionNum++;
                    if (partition.CurrentOwner == "1") worker1PartitionNum++;
                    if (partition.CurrentOwner == "2") worker2PartitionNum++;
                    if (partition.CurrentOwner == "3") worker3PartitionNum++;
                    if ((worker0PartitionNum == worker1PartitionNum) && (worker2PartitionNum == worker3PartitionNum) && (worker0PartitionNum == worker2PartitionNum))
                    {
                        isBalanced = true;
                        Assert.AreEqual(1, worker0PartitionNum);
                    }
                }
                
            }
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.Elapsed <= timeout);
            Assert.IsTrue(isBalanced);

            var tasks = partitionManagerList.Select(partitionManager => partitionManager.StopAsync());
            await Task.WhenAll(tasks);
            await testTable.DeleteAsync();
        }

        [TestCategory("DisabledInCI")]
        [TestMethod]
        //Starts with one workers and four partitions.And then add three more workers.
        //Test that each worker can acquire four partitions. 
        //Test that the lease tranfer will take no longer than 30 sec.
        public async Task TestAddThreeWorkersWithOneWorkerAndFourPartitions()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(30);
            Stopwatch stopwatch = new Stopwatch();
            TableClient testTable = await CreateTestTable("UseDevelopmentStorage = true", "TestAddThreeWorkersWithOneWorkerAndFourPartitions", 4);
            IList<TablePartitionManager> partitionManagerList = new List<TablePartitionManager>();

            int workerNum = 4;
            for (int i = 0; i < workerNum; i++)
            {
                TablePartitionManager partitionManager = new TablePartitionManager { partitionTable = testTable, workerName = i.ToString() };
                partitionManagerList.Add(partitionManager);
            }

            partitionManagerList[0].StartAsync();
            await Task.Delay(1000);
            partitionManagerList[1].StartAsync();
            partitionManagerList[2].StartAsync();
            partitionManagerList[3].StartAsync();

            stopwatch.Start();
            bool isBalanced = false;

            while (stopwatch.Elapsed < timeout && !isBalanced)
            {
                int worker0PartitionNum = 0;
                int worker1PartitionNum = 0;
                int worker2PartitionNum = 0;
                int worker3PartitionNum = 0;

                Pageable<TableLease> partitions = testTable.Query<TableLease>();
                foreach (var partition in partitions)
                {
                    if (partition.CurrentOwner == "0") worker0PartitionNum++;
                    if (partition.CurrentOwner == "1") worker1PartitionNum++;
                    if (partition.CurrentOwner == "2") worker2PartitionNum++;
                    if (partition.CurrentOwner == "3") worker3PartitionNum++;
                }
                if ((worker0PartitionNum == worker1PartitionNum) && (worker2PartitionNum == worker3PartitionNum) && (worker0PartitionNum == worker2PartitionNum))
                {
                    isBalanced = true;
                    Assert.AreEqual(1, worker0PartitionNum);
                }
            }
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.Elapsed <= timeout);
            Assert.IsTrue(isBalanced);

            var tasks = partitionManagerList.Select(partitionManager => partitionManager.StopAsync());
            await Task.WhenAll(tasks);
            await testTable.DeleteAsync();
        }

        [TestCategory("DisabledInCI")]
        [TestMethod]
        //Starts with four workers and four partitions. And then add four more workers.
        //Test that the added workers will do nothing. 
        public async Task TestAddFourWorkersWithFourWorkersAndFourPartitions()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(15);
            Stopwatch stopwatch = new Stopwatch();
            TableClient testTable = await CreateTestTable("UseDevelopmentStorage = true", "TestAddFourWorkersWithFourWorkersAndFourPartitions", 4);
            IList<TablePartitionManager> partitionManagerList = new List<TablePartitionManager>();

            int workerNum = 8;
            for (int i = 0; i < workerNum; i++)
            {
                TablePartitionManager partitionManager = new TablePartitionManager { partitionTable = testTable, workerName = i.ToString() };
                partitionManagerList.Add(partitionManager);
            }

            for (int i=0; i < 4; i++) 
            {
                partitionManagerList[i].StartAsync();
            }
            await Task.Delay(30000);
            
            for (int i = 4; i < 8; i++)
            {
                partitionManagerList[i].StartAsync();
            }

            stopwatch.Start();

            while (stopwatch.Elapsed < timeout )
            {
                int worker4PartitionNum = 0;
                int worker5PartitionNum = 0;
                int worker6PartitionNum = 0;
                int worker7PartitionNum = 0;

                Pageable<TableLease> partitions = testTable.Query<TableLease>();
                foreach (var partition in partitions)
                {
                    if (partition.CurrentOwner == "4" || partition.NextOwner == "4") worker4PartitionNum++;
                    if (partition.CurrentOwner == "5" || partition.NextOwner == "5") worker5PartitionNum++;
                    if (partition.CurrentOwner == "6" || partition.NextOwner == "6") worker6PartitionNum++;
                    if (partition.CurrentOwner == "7" || partition.NextOwner == "7") worker7PartitionNum++;
                }

                Assert.AreEqual(0, worker4PartitionNum);
                Assert.AreEqual(0, worker5PartitionNum);
                Assert.AreEqual(0, worker6PartitionNum);
                Assert.AreEqual(0, worker7PartitionNum);
            }
            stopwatch.Stop();

            var tasks = partitionManagerList.Select(partitionManager => partitionManager.StopAsync());
            await Task.WhenAll(tasks);
            await testTable.DeleteAsync();
        }

        [TestCategory("DisabledInCI")]
        [TestMethod]
        //Start with four workers and four partitions. And then sacle down to three workers.
        //Test that partitions will be rebalance between the three workers, which is one worker will have two, and the other two both have one. 
        public async Task TestScalingDownToThreeWorkers()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(5);
            Stopwatch stopwatch = new Stopwatch();
            TableClient testTable = await CreateTestTable("UseDevelopmentStorage = true", "TestScalingDownToThreeWorkers", 4);
            IList<TablePartitionManager> partitionManagerList = new List<TablePartitionManager>();

            int workerNum = 4;
            for (int i = 0; i < workerNum; i++)
            {
                TablePartitionManager partitionManager = new TablePartitionManager { partitionTable = testTable, workerName = i.ToString() };
                partitionManagerList.Add(partitionManager);
                partitionManager.StartAsync();
            }

            await Task.Delay(30000);
            await partitionManagerList[3].StopAsync();
            partitionManagerList.RemoveAt(3);
            
            stopwatch.Start();
            bool isBalanced = false;

            while (stopwatch.Elapsed < timeout && !isBalanced)
            {
                int worker0PartitionNum = 0;
                int worker1PartitionNum = 0;
                int worker2PartitionNum = 0;

                Pageable<TableLease> partitions = testTable.Query<TableLease>();
                foreach (var partition in partitions)
                {
                    if (partition.CurrentOwner == null) break;
                    if (partition.CurrentOwner == "0") worker0PartitionNum++;
                    if (partition.CurrentOwner == "1") worker1PartitionNum++;
                    if (partition.CurrentOwner == "2") worker2PartitionNum++;
                  
                }
                
                if (worker0PartitionNum == 2)
                {
                    isBalanced = true;
                    Assert.AreEqual(1, worker1PartitionNum);
                    Assert.AreEqual(1, worker2PartitionNum);
                }

                if (worker1PartitionNum == 2)
                {
                    isBalanced = true;
                    Assert.AreEqual(1, worker0PartitionNum);
                    Assert.AreEqual(1, worker2PartitionNum);
                }

                if (worker2PartitionNum == 2)
                {
                    isBalanced = true;
                    Assert.AreEqual(1, worker1PartitionNum);
                    Assert.AreEqual(1, worker0PartitionNum);
                }
            }
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.Elapsed <= timeout);
            Assert.IsTrue(isBalanced);

            var tasks = partitionManagerList.Select(partitionManager => partitionManager.StopAsync());
            await Task.WhenAll(tasks);
            await testTable.DeleteAsync();
        }

        [TestCategory("DisabledInCI")]
        [TestMethod]
        //Start with four workers and four partitions. And then sacle down to one workers.
        //Test that the left one worker will take the four partitions.
        public async Task TestScalingDownToOneWorker()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(5);
            Stopwatch stopwatch = new Stopwatch();
            TableClient testTable = await CreateTestTable("UseDevelopmentStorage = true", "TestScalingDownToOneWorker", 4);
            IList<TablePartitionManager> partitionManagerList = new List<TablePartitionManager>();

            int workerNum = 4;
            for (int i = 0; i < workerNum; i++)
            {
                TablePartitionManager partitionManager = new TablePartitionManager { partitionTable = testTable, workerName = i.ToString() };
                partitionManagerList.Add(partitionManager);
                partitionManager.StartAsync();
            }

            await Task.Delay(40000);

            IList<Task> tasks = new List<Task>();
            tasks.Add(partitionManagerList[1].StopAsync());
            tasks.Add(partitionManagerList[2].StopAsync());
            tasks.Add(partitionManagerList[3].StopAsync());
            await Task.WhenAll(tasks);

            stopwatch.Start();
            bool isBalanced = false;

            while (stopwatch.Elapsed < timeout && !isBalanced)
            {
                int worker0PartitionNum = 0;

                Pageable<TableLease> partitions = testTable.Query<TableLease>();
                foreach (var partition in partitions)
                {
                    if (partition.CurrentOwner == "0") worker0PartitionNum++;

                }

                if (worker0PartitionNum == 4)
                {
                    isBalanced = true;
                }
            }
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.Elapsed <= timeout);
            Assert.IsTrue(isBalanced);

            await partitionManagerList[0].StopAsync();
            await testTable.DeleteAsync();
        }

        [TestCategory("DisabledInCI")]
        [TestMethod]
        //Start with four workers and four partitions. Then kill one worker.
        //Test that the partitions will be rebalanced among he three left workers.
        public async Task TestKillOneWorker()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(60);
            Stopwatch stopwatch = new Stopwatch();
            TableClient testTable = await CreateTestTable("UseDevelopmentStorage = true", "TestKillOneWorker", 4);
            IList<TablePartitionManager> partitionManagerList = new List<TablePartitionManager>();
            CancellationTokenSource source = new CancellationTokenSource();
            CancellationToken token = source.Token;

            int workerNum = 4;
            for (int i = 0; i < workerNum; i++)
            {
                TablePartitionManager partitionManager = new TablePartitionManager { partitionTable = testTable, workerName = i.ToString() };
                partitionManagerList.Add(partitionManager);
            }
            partitionManagerList[3].source = source;
            for (int i = 0; i < workerNum; i++)
            {
                partitionManagerList[i].StartAsync();
            }
            await Task.Delay(30000);

            source.Cancel();
            partitionManagerList.RemoveAt(3);
            stopwatch.Start();
            bool isBalanced = false;

            while (stopwatch.Elapsed < timeout && !isBalanced)
            {
                int worker0PartitionNum = 0;
                int worker1PartitionNum = 0;
                int worker2PartitionNum = 0;
                int worker3PartitionNum = 0;
                Pageable<TableLease> partitions = testTable.Query<TableLease>();
                foreach (var partition in partitions)
                {
                    if (partition.CurrentOwner == null) break;
                    if (partition.CurrentOwner == "0") worker0PartitionNum++;
                    if (partition.CurrentOwner == "1") worker1PartitionNum++;
                    if (partition.CurrentOwner == "2") worker2PartitionNum++;
                    if (partition.CurrentOwner == "3") worker3PartitionNum++;
                }

                if (worker0PartitionNum == 2)
                {
                    isBalanced = true;
                    Assert.AreEqual(1, worker1PartitionNum);
                    Assert.AreEqual(1, worker2PartitionNum);
                }
                if (worker1PartitionNum == 2)
                {
                    isBalanced = true;
                    Assert.AreEqual(1, worker0PartitionNum);
                    Assert.AreEqual(1, worker2PartitionNum);
                }
                if (worker2PartitionNum == 2)
                {
                    isBalanced = true;
                    Assert.AreEqual(1, worker1PartitionNum);
                    Assert.AreEqual(1, worker0PartitionNum);
                }
            }
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.Elapsed <= timeout);
            Assert.IsTrue(isBalanced);

            var stopWorkers = partitionManagerList.Select(partitionManager => partitionManager.StopAsync());
            await Task.WhenAll(stopWorkers);
            await testTable.DeleteAsync();
        }

        [TestCategory("DisabledInCI")]
        [TestMethod]
        //Start with four workers and four partitions. Then kill three workers.
        //Test that the left worker will take all the partitions.
        public async Task TestKillThreeWorker()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(60);
            Stopwatch stopwatch = new Stopwatch();
            TableClient testTable = await CreateTestTable("UseDevelopmentStorage = true", "TestKillThreeWorker", 4);
            IList<TablePartitionManager> partitionManagerList = new List<TablePartitionManager>();
            CancellationTokenSource source = new CancellationTokenSource();
            CancellationToken token = source.Token;

            int workerNum = 4;
            for (int i = 0; i < workerNum; i++)
            {
                TablePartitionManager partitionManager = new TablePartitionManager { partitionTable = testTable, workerName = i.ToString() };
                partitionManagerList.Add(partitionManager);
            }
            partitionManagerList[3].source = source;
            partitionManagerList[2].source = source;
            partitionManagerList[1].source = source;

            for (int i = 0; i < workerNum; i++)
            {
                partitionManagerList[i].StartAsync();
            }
            await Task.Delay(30000);

            source.Cancel();
            stopwatch.Start();
            bool isBalanced = false;

            while (stopwatch.Elapsed < timeout && !isBalanced)
            {
                int worker0PartitionNum = 0;
                Pageable<TableLease> partitions = testTable.Query<TableLease>();
                foreach (var partition in partitions)
                {
                    if (partition.CurrentOwner == "0") worker0PartitionNum++;
                }

                if (worker0PartitionNum == 4)
                {
                    isBalanced = true;
                }
            }
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.Elapsed <= timeout);
            Assert.IsTrue(isBalanced);

            var stopWorkers = partitionManagerList.Select(partitionManager => partitionManager.StopAsync());
            await Task.WhenAll(stopWorkers);
            await testTable.DeleteAsync();
        }


        async Task<TableClient> CreateTestTable(string storageAccount, string tableName, int leaseNum)
        {
            TableClient testTable = new TableClient(storageAccount, tableName);
            await testTable.CreateIfNotExistsAsync();
            await AddTestTableLease(testTable, leaseNum);
            return testTable;
        }
        async Task AddTestTableLease(TableClient testTable, int leaseNum)
        {
            for (int i = 0; i < leaseNum; i++)
            {
                TableLease lease = new TableLease()
                {
                    PartitionKey = "",
                    RowKey = "controlqueue-" + i.ToString("D2")
                };
                await testTable.AddEntityAsync(lease);
            }
        }
    }
}
