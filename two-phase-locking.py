'''
Author - Rushikesh Sureshkumar Patel
==> Concurrency Control using Two Phase Locking 
    Methods Implemented:
    1. Wound & Wait
    2. Cautious Waiting
    3. No-Wait
'''

import pandas as pd
from tabulate import tabulate

def read_input(filename):
    global transaction_table
    global lock_table
    list = []
    file = open(filename,"r")
    for line in file:
        line = line.rstrip("\n")
        print("Operation", line)
        main(line)
        print("Transaction Table:")
        print(tabulate(transaction_table, headers='keys', tablefmt='psql',showindex="never"))
        print("Lock Table:")
        print(tabulate(lock_table, headers='keys', tablefmt='psql',showindex="never"))

def add_transaction(name):
    global transaction_table
    global lock_table
    transaction = "T"+str(name[1])
    if transaction not in transaction_table["T-ID"].values:
        new_row = {"T-ID":"T"+str(name[1]),"TimeStamp":name[1],"State": "Active","Blocked-by":[],"Blocked-Operations":[]}
        transaction_table = transaction_table.append(new_row,ignore_index=True)
        print("Begin Transaction: T"+str(name[1]))
    
def read_operation(name):
    global transaction_table
    global lock_table
    global method

    if transaction_table.iloc[int(name[1])-1]["State"] == "Active":
        if name[3] in lock_table["Data-Item"].values:
            row_index = int(lock_table[lock_table["Data-Item"] == name[3]].index[0])
            # Following condition checks whether the data-item in the Lock-Table is in Read mode or not
            if lock_table.iloc[row_index]["Lock-Mode"] == "R":
                # If it is, this will simply append the T-ID to the list of T-IDs (shared lock)
                lock_table.iloc[row_index]["T-ID"].append("T"+str(name[1]))
            
            # This condition check whether the dat-item in the lock table is in Write Mode
            if lock_table.loc[row_index]["Lock-Mode"] == "W":
                # If true, this will lead to a conflicting mode hence we check the timestamp with the transaction currently holding the lock
                transaction = "T"+str(name[1])
                current_transaction_row_index = int(transaction_table[transaction_table["T-ID"] == transaction].index[0])
                transaction_timestamp = transaction_table.iloc[current_transaction_row_index]["TimeStamp"]
                # Following condtion checks, if the current transaction holds the lock
                # Trying the new method
                if method == "1":
                    if transaction in lock_table.iloc[row_index]["T-ID"]:
                        lock_table.iloc[row_index]["Lock-Mode"] = name[0].upper()
                        # lock_table.iloc[row_index]["T-ID"].append(transaction)
                    else:
                        lock_holding_transaction = (lock_table.iloc[row_index]["T-ID"])[0]
                        row_index_of_comparing_transaction = int(transaction_table[transaction_table["T-ID"] == lock_holding_transaction].index[0])
                        timestamp_of_comparing_transaction = transaction_table.iloc[row_index_of_comparing_transaction]["TimeStamp"]
                        if transaction_timestamp < timestamp_of_comparing_transaction:
                        # i gets aborted
                            transaction_table.iloc[row_index_of_comparing_transaction]["State"] = "Aborted"
                            transaction_table.iloc[row_index_of_comparing_transaction]["Blocked-by"] = []
                            transaction_table.iloc[row_index_of_comparing_transaction]["Blocked-Operations"] = "None"
                            lock_table.iloc[row_index]["T-ID"].remove(lock_holding_transaction)
                        else:
                                
                            transaction_table.iloc[current_transaction_row_index]["State"] = "Blocked"
                            transaction_table.iloc[current_transaction_row_index]["Blocked-by"].append(str(lock_holding_transaction))
                            if name not in transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"]:
                                transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"].append(name)
                        
                        if not lock_table.iloc[row_index]["T-ID"]:
                            lock_table.iloc[row_index]["Lock-Mode"] = name[0].upper()
                            lock_table.iloc[row_index]["T-ID"].append(transaction)
                elif method == "2":
                    #Wait and Die
                    if transaction in lock_table.iloc[row_index]["T-ID"]:
                        lock_table.iloc[row_index]["Lock-Mode"] = name[0].upper()
                        
                    else:
                        lock_holding_transaction = (lock_table.iloc[row_index]["T-ID"])[0]
                        row_index_of_comparing_transaction = int(transaction_table[transaction_table["T-ID"] == lock_holding_transaction].index[0])
                        timestamp_of_comparing_transaction = transaction_table.iloc[row_index_of_comparing_transaction]["TimeStamp"]
                        if transaction_timestamp > timestamp_of_comparing_transaction:
                        # i gets aborted
                            transaction_table.iloc[row_index_of_comparing_transaction]["State"] = "Aborted"
                            transaction_table.iloc[row_index_of_comparing_transaction]["Blocked-by"] = []
                            transaction_table.iloc[row_index_of_comparing_transaction]["Blocked-Operations"] = "None"
                            lock_table.iloc[row_index]["T-ID"].remove(lock_holding_transaction)
                            for j in lock_table["T-ID"].values:
                                    if lock_holding_transaction in j:
                                        j.remove(lock_holding_transaction)
                                        if not j:
                                            data_item_row = (list(lock_table["T-ID"].values)).index(j)
                                            lock_table = lock_table.drop(lock_table.index[data_item_row])
                            lock_table = lock_table.reset_index(drop = True)
                        else:
                                
                            transaction_table.iloc[current_transaction_row_index]["State"] = "Blocked"
                            transaction_table.iloc[current_transaction_row_index]["Blocked-by"].append(str(lock_holding_transaction))
                            if name not in transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"]:
                                transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"].append(name)
                        
                        if not lock_table.iloc[row_index]["T-ID"]:
                            lock_table.iloc[row_index]["Lock-Mode"] = name[0].upper()
                            lock_table.iloc[row_index]["T-ID"].append(transaction)
                elif method == "3":
                    # No-waiting
                    if transaction in lock_table.iloc[row_index]["T-ID"]:
                        lock_table.iloc[row_index]["Lock-Mode"] = name[0].upper()
                        
                    else:
                        lock_holding_transaction = (lock_table.iloc[row_index]["T-ID"])[0]
                        row_index_of_comparing_transaction = int(transaction_table[transaction_table["T-ID"] == lock_holding_transaction].index[0])
                        transaction_table.iloc[row_index_of_comparing_transaction]["State"] = "Aborted"
                        transaction_table.iloc[row_index_of_comparing_transaction]["Blocked-by"] = []
                        transaction_table.iloc[row_index_of_comparing_transaction]["Blocked-Operations"] = "None"
                        lock_table.iloc[row_index]["T-ID"].remove(lock_holding_transaction)
                        for i in lock_table["T-ID"].values:
                            if lock_holding_transaction in i:
                                i.remove(lock_holding_transaction)
                                if not i:
                                    data_item_row = (list(lock_table["T-ID"].values)).index(i)
                                    lock_table = lock_table.drop(lock_table.index[data_item_row])
                        lock_table = lock_table.reset_index(drop = True)
 
                        if not lock_table.iloc[row_index]["T-ID"]:
                            lock_table.iloc[row_index]["Lock-Mode"] = name[0].upper()
                            lock_table.iloc[row_index]["T-ID"].append(transaction)
        else:
            add_row = {"Data-Item": str(name[3]), "Lock-Mode":name[0].upper(), "T-ID": ["T"+str(name[1])]}
            lock_table = lock_table.append(add_row, ignore_index=True)
            
    if transaction_table.iloc[int(name[1])-1]["State"] == "Blocked" and name not in transaction_table.iloc[int(name[1])-1]["Blocked-Operations"]:
        row_index = int(transaction_table[transaction_table["T-ID"] == "T"+str(name[1])].index[0])
        transaction_table.iloc[row_index]["Blocked-Operations"].append(name)
        
    if transaction_table.iloc[int(name[1])-1]["State"] == "Aborted":
        print("T"+str(name[1]),"is already aborted. So no changes in the tables.")

def write_operation(name):
    global transaction_table
    global lock_table
    global  method
    if transaction_table.iloc[int(name[1])-1]["State"] == "Active":
        transaction = "T"+str(name[1])
        current_transaction_row_index = int(transaction_table[transaction_table["T-ID"] == transaction].index[0])
        transaction_timestamp = transaction_table.iloc[current_transaction_row_index]["TimeStamp"]
        # Data-Item Exists in the Lock table
        if name[3] in lock_table["Data-Item"].values:
            row_index = int(lock_table[lock_table["Data-Item"] == name[3]].index[0])
            if method == "1":
                # Current Transaction accessing it
                if transaction in lock_table.iloc[row_index]["T-ID"]:
                    lock_table.iloc[row_index]["T-ID"].remove(transaction)
                    # Condition to check, Only lock is held by current transaction
                    if not lock_table.iloc[row_index]["T-ID"]:
                        # Simply upgrade the lock
                        lock_table.iloc[row_index]["Lock-Mode"] = name[0].upper()
                        lock_table.iloc[row_index]["T-ID"].append(transaction)
                    else:
                        for i in list(lock_table.iloc[row_index]["T-ID"]):
                            row_index_of_comparing_transaction = int(transaction_table[transaction_table["T-ID"] == i].index[0])
                            timestamp_of_comparing_transaction = transaction_table.iloc[row_index_of_comparing_transaction]["TimeStamp"]
                            if transaction_timestamp < timestamp_of_comparing_transaction:
                            # i gets aborted
                                transaction_table.iloc[row_index_of_comparing_transaction]["State"] = "Aborted"
                                transaction_table.iloc[row_index_of_comparing_transaction]["Blocked-by"] = []
                                transaction_table.iloc[row_index_of_comparing_transaction]["Blocked-Operations"] = "None"
                                lock_table.iloc[row_index]["T-ID"].remove(i)
                                temp = (lock_table["T-ID"].values)[:]
                                for j in temp:
                                    if i in j:
                                        location = list(temp).index(j)
                                        (lock_table["T-ID"].values)[location].remove(i)
                                        if not (lock_table["T-ID"].values)[location]:
                                            data_item_row = (list(lock_table["T-ID"].values)).index((lock_table["T-ID"].values)[location])
                                            lock_table = lock_table.drop(lock_table.index[data_item_row])
                                lock_table = lock_table.reset_index(drop = True)
                            else:
                            # current transaction waits
                            
                                transaction_table.iloc[current_transaction_row_index]["State"] = "Blocked"
                                transaction_table.iloc[current_transaction_row_index]["Blocked-by"].append(str(i))
                                if name not in transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"]:
                                    transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"].append(name)
                        if not lock_table.iloc[row_index]["T-ID"]:
                            lock_table.iloc[row_index]["Lock-Mode"] = name[0].upper()
                            lock_table.iloc[row_index]["T-ID"].append(transaction)
                        else:
                            lock_table.iloc[row_index]["T-ID"].append(transaction)
                
                else:
                    
                    for i in list(lock_table.iloc[row_index]["T-ID"]):
                        row_index_of_comparing_transaction = int(transaction_table[transaction_table["T-ID"] == i].index[0])
                        timestamp_of_comparing_transaction = transaction_table.iloc[row_index_of_comparing_transaction]["TimeStamp"]
                        if transaction_timestamp < timestamp_of_comparing_transaction:
                            # i gets aborted
                            transaction_table.iloc[row_index_of_comparing_transaction]["State"] = "Aborted"
                            transaction_table.iloc[row_index_of_comparing_transaction]["Blocked-by"] = []
                            transaction_table.iloc[row_index_of_comparing_transaction]["Blocked-Operations"] = "None"
                            lock_table.iloc[row_index]["T-ID"].remove(i)
                            temp = (lock_table["T-ID"].values)[:]
                            for j in temp:
                                    if i in j:
                                        location = list(temp).index(j)
                                        (lock_table["T-ID"].values)[location].remove(i)
                                        if not (lock_table["T-ID"].values)[location]:
                                            data_item_row = (list(lock_table["T-ID"].values)).index((lock_table["T-ID"].values)[location])
                                            lock_table = lock_table.drop(lock_table.index[data_item_row])
                            lock_table = lock_table.reset_index(drop = True)
                        else:
                            # current transaction waits
                            
                            transaction_table.iloc[current_transaction_row_index]["State"] = "Blocked"
                            transaction_table.iloc[current_transaction_row_index]["Blocked-by"].append(str(i))
                            if name not in transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"]:
                                transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"].append(name)
                    #If every Transaction aborted then the current operation is performed and respective transaction locks the data
                    if not lock_table.iloc[row_index]["T-ID"]:
                        lock_table.iloc[row_index]["Lock-Mode"] = name[0].upper()
                        lock_table.iloc[row_index]["T-ID"].append(transaction)
            
            elif method == "2":
                # Current Transaction accessing it
                if transaction in lock_table.iloc[row_index]["T-ID"]:
                    lock_table.iloc[row_index]["T-ID"].remove(transaction)
                    # Condition to check, Only lock is held by current transaction
                    if not lock_table.iloc[row_index]["T-ID"]:
                        # Simply upgrade the lock
                        lock_table.iloc[row_index]["Lock-Mode"] = name[0].upper()
                        lock_table.iloc[row_index]["T-ID"].append(transaction)
                    else:
                        copy = lock_table.iloc[row_index]["T-ID"][:]
                        for i in list(lock_table.iloc[row_index]["T-ID"]):
                            row_index_of_comparing_transaction = int(transaction_table[transaction_table["T-ID"] == i].index[0])
                            timestamp_of_comparing_transaction = transaction_table.iloc[row_index_of_comparing_transaction]["TimeStamp"]
                            if transaction_timestamp > timestamp_of_comparing_transaction:
                            # i gets aborted
                                transaction_table.iloc[row_index_of_comparing_transaction]["State"] = "Aborted"
                                transaction_table.iloc[row_index_of_comparing_transaction]["Blocked-by"] = []
                                transaction_table.iloc[row_index_of_comparing_transaction]["Blocked-Operations"] = "None"
                                lock_table.iloc[row_index]["T-ID"].remove(i)
                                temp = (lock_table["T-ID"].values)[:]
                                for j in temp:
                                    if i in j:
                                        location = list(temp).index(j)
                                        (lock_table["T-ID"].values)[location].remove(i)
                                        if not (lock_table["T-ID"].values)[location]:
                                            data_item_row = (list(lock_table["T-ID"].values)).index((lock_table["T-ID"].values)[location])
                                            lock_table = lock_table.drop(lock_table.index[data_item_row])
                                lock_table = lock_table.reset_index(drop = True)
                            else:
                            # current transaction waits
                            
                                transaction_table.iloc[current_transaction_row_index]["State"] = "Blocked"
                                transaction_table.iloc[current_transaction_row_index]["Blocked-by"].append(str(i))
                                if name not in transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"]:
                                    transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"].append(name)
                        if not lock_table.iloc[row_index]["T-ID"]:
                            lock_table.iloc[row_index]["Lock-Mode"] = name[0].upper()
                            lock_table.iloc[row_index]["T-ID"].append(transaction)
                        else:
                            lock_table.iloc[row_index]["T-ID"].append(transaction)
                
                else:
                    for i in list(lock_table.iloc[row_index]["T-ID"]):
                        row_index_of_comparing_transaction = int(transaction_table[transaction_table["T-ID"] == i].index[0])
                        timestamp_of_comparing_transaction = transaction_table.iloc[row_index_of_comparing_transaction]["TimeStamp"]
                        if transaction_timestamp > timestamp_of_comparing_transaction:
                            # i gets aborted
                            transaction_table.iloc[row_index_of_comparing_transaction]["State"] = "Aborted"
                            transaction_table.iloc[row_index_of_comparing_transaction]["Blocked-by"] = []
                            transaction_table.iloc[row_index_of_comparing_transaction]["Blocked-Operations"] = "None"
                            lock_table.iloc[row_index]["T-ID"].remove(i)
                            for j in lock_table["T-ID"].values:
                                    if i in j:
                                        j.remove(i)
                                        if not j:
                                            data_item_row = (list(lock_table["T-ID"].values)).index(j)
                                            lock_table = lock_table.drop(lock_table.index[data_item_row])
                            lock_table = lock_table.reset_index(drop = True)
                        else:
                            # current transaction waits
                            
                            transaction_table.iloc[current_transaction_row_index]["State"] = "Blocked"
                            transaction_table.iloc[current_transaction_row_index]["Blocked-by"].append(str(i))
                            if name not in transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"]:
                                transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"].append(name)
                    #If every Transaction aborted then the current operation is performed and respective transaction locks the data
                    if not lock_table.iloc[row_index]["T-ID"]:
                        lock_table.iloc[row_index]["Lock-Mode"] = name[0].upper()
                        lock_table.iloc[row_index]["T-ID"].append(transaction)
            elif method == "3":
                for i in list(lock_table.iloc[row_index]["T-ID"]):
                    if i != transaction:
                        row_index_of_comparing_transaction = int(transaction_table[transaction_table["T-ID"] == i].index[0])
                        timestamp_of_comparing_transaction = transaction_table.iloc[row_index_of_comparing_transaction]["TimeStamp"]
                        transaction_table.iloc[row_index_of_comparing_transaction]["State"] = "Aborted"
                        transaction_table.iloc[row_index_of_comparing_transaction]["Blocked-by"] = []
                        transaction_table.iloc[row_index_of_comparing_transaction]["Blocked-Operations"] = "None"
                        lock_table.iloc[row_index]["T-ID"].remove(i)
                        temp = (lock_table["T-ID"].values)[:]
                        for j in temp:
                            if i in j:
                                location = list(temp).index(j)
                                (lock_table["T-ID"].values)[location].remove(i)
                                if not (lock_table["T-ID"].values)[location]:
                                    data_item_row = (list(lock_table["T-ID"].values)).index((lock_table["T-ID"].values)[location])
                                    lock_table = lock_table.drop(lock_table.index[data_item_row])
                        lock_table = lock_table.reset_index(drop = True)
                lock_table.iloc[row_index]["Lock-Mode"] = name[0].upper()


        else:
            add_row = {"Data-Item": str(name[3]), "Lock-Mode":name[0].upper(), "T-ID": ["T"+str(name[1])]}
            lock_table = lock_table.append(add_row, ignore_index=True)
            
    if transaction_table.iloc[int(name[1])-1]["State"] == "Blocked" and name not in transaction_table.iloc[int(name[1])-1]["Blocked-Operations"]:
        row_index = int(transaction_table[transaction_table["T-ID"] == "T"+str(name[1])].index[0])
        transaction_table.iloc[row_index]["Blocked-Operations"].append(name)
    
    if transaction_table.iloc[int(name[1])-1]["State"] == "Aborted":
        print("T"+str(name[1]),"is already aborted. So no changes in the tables.")
            
def end_transaction(name):
    global transaction_table
    global lock_table 
    
    transaction = "T"+str(name[1])
    if transaction_table.iloc[int(name[1])-1]["State"] == "Active":
        #Status change to committed
        transaction_table.iloc[int(name[1])-1]["State"] = "Committed"
        # Release locks
        for i in list(lock_table["T-ID"].values):
            if transaction in i:
                i.remove(transaction)
                if not i:
                    data_item_row = (list(lock_table["T-ID"].values)).index(i)
                    lock_table = lock_table.drop(lock_table.index[data_item_row])
        lock_table = lock_table.reset_index(drop = True)
        # continue the blocked transactions by passing them through main function
        if list(transaction_table["Blocked-by"].values):
            for j in transaction_table["Blocked-by"].values:
                if transaction in j:
                    transaction_row = (list(transaction_table["Blocked-by"].values)).index(j)
                    j.remove(transaction)
                    transaction_table.iloc[transaction_row]['State'] = "Active"
                    for k in list(transaction_table.iloc[transaction_row]["Blocked-Operations"][:]):
                        main(k)
                    if transaction_table.iloc[transaction_row]["Blocked-by"] == []:
                        transaction_table.iloc[transaction_row]["Blocked-Operations"] = []
    
    if transaction_table.iloc[int(name[1])-1]["State"] == "Blocked" and not (name in transaction_table.iloc[int(name[1])-1]["Blocked-Operations"]):
        row_index = int(transaction_table[transaction_table["T-ID"] == "T"+str(name[1])].index[0])
        transaction_table.iloc[row_index]["Blocked-Operations"].append(name)

    if transaction_table.iloc[int(name[1])-1]["State"] == "Aborted":
        print(transaction,"is already aborted. So no changes in the tables.")


def main(operation):
    if operation[0] == "b":
        add_transaction(operation)
    if operation[0] == "r":
        read_operation(operation)
    if operation[0] == "w":
        write_operation(operation)
    if operation[0] == "e":
        end_transaction(operation)


if __name__ == "__main__":
    file = "input-4.txt"
    print("Current file:",file)
    method = input("Please select the method of concurrency control to implement:\n1. Wound & Wait\n2. Cautious Waiting\n3. No-Waiting\nSelect 1,2 or 3.\nType here: ")
    print("You selected",method,"\n\nBegin Transaction")
    transaction_table = pd.DataFrame(columns=["T-ID", "TimeStamp","State", "Blocked-by", "Blocked-Operations"])
    lock_table = pd.DataFrame(columns=["Data-Item","Lock-Mode","T-ID"])
    if method == "1":
        read_input(file)
    elif method == "2":
        read_input(file)
    elif method == "3":
        read_input(file)
    else:
        print("Please select one of:\n1,2 & 3 and run again.")
        print("\n\n\n")
        

