#Service Fabric Table Service
An ESENT-based database on Service Fabric. Demonstrates how to write a distributed, reliable database which uses Service Fabric for replication.

The `ReliableTable<TKey, TValue>` class can take part in transactions along with Service Fabric's existing `IReliableDictionary` & `IReliableQueue` classes.

## [Twitter: @ReubenBond](https://twitter.com/reubenbond) :)

Example:
In your StatefulService's `RunAsync` method, obtain a reference to a table:
```C#
var journal = await this.StateManager.GetOrAddAsync<ReliableTable<string, byte[]>>("journal");
```
Then, access the table from one of your service's methods like so 
```C#
public async Task Insert(string key, string partition, byte[] value)
{
	using (var tx = this.StateManager.CreateTransaction())
	{
		journal.SetValue(tx, key, value);
		await tx.CommitAsync();
	}
}
```
Ideas for improvement:
* Use a wholly managed database to avoid calls into native code.
* Alternatively: chunkify the marshalling via a native library.
* Use ProtoBuf for serialization of operations, instead of manual binary serialization.
* Profile & improve performance.

For a higher performance database, implement the `IStateProvider2` interface manually and replicate the database journal instead of the application-level operations (is that possible with ESENT?). This implementation uses the `TransactionalReplicator` from Service Fabric, which maintains its own transaction log, ESENT also maintains a transaction log internally, so there is a duplication of effort.

I give zero guarantees about any of this.

Credits to [ManagedEsent](https://managedesent.codeplex.com/) which this project borrows from and leverages.