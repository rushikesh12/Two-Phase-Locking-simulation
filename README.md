# Two-Phase-Locking-simulation
Implementation of Two Phase Locking for concurrency control using different methods in Python environment.

Concurrency control methods are used to perserve database consistency while transactions are executing concurrently. The methods that are used in this program are:
1. Wound & Wait
2. Wait & Die
3. No Waiting
4. Cautious Waiting (Deadlock Free)

### Wound-Wait & Wait-Die
These methods are based on timestamps. When a transaction begins the database automatically allots the current time as the timestamp to the respective transaction.
Wound-Wait is based on the thinking to give preference to the older transations (the ones which started before i.e. before in time).
Wait-Die is based on the thinnking to give preferences to the younger transactions (the ones which started last).

As time is ever increasing, younger transactions have greater timestamps and consequently older transactions have lesser timestamps.

### No Waiting
In this method, a particular transaction does not wait for any transaction to obtain a lock and aborts other transactions when in conflicting mode.
Due to this, many transactions are aborted.

### Cautious Waiting
In this method, when in a conflicting mode, the transaction checks whether the lock holding transaction is blocked or not. If it is blocked, lock requesting transaction aborts itself, otherwise it wait for the lock holding transaction to finish. In this way, we are guaranteed to never have a deadlock situation.

## Dependencies
Pandas and Tabulate
Pandas - For creating Transaction Table and Lock Table
Tabulate - To print out the tables in a pretty format 
Installing Pandas and Tabulate using pip:

```bash
pip install pandas
pip install tabulate
```