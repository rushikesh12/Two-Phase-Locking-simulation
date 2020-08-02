'''
Author - Rushikesh Sureshkumar Patel
==> Concurrency Control using Two Phase Locking (Rigorous)
    Methods Implemented:
    1. Wound & Wait
    2. Wait & Die
    3. No-Waiting
    4. Cautious Waiting
    
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

def main(operation):
    if operation[0] == "b":
        add_transaction(operation)
    if operation[0] == "r":
        read_operation(operation)
    if operation[0] == "w":
        write_operation(operation)
    if operation[0] == "e":
        end_transaction(operation)


def add_transaction(name):
    global transaction_table
    global lock_table
    transaction = "T"+str(name[1])
    if transaction not in transaction_table["T-ID"].values:
        new_row = {"T-ID":"T"+str(name[1]),"TimeStamp":name[1],"State": "Active","Blocked-by":[],"Blocked-Operations":[]}
        transaction_table = transaction_table.append(new_row,ignore_index=True)
        print("Begin Transaction: T"+str(name[1]))
    

def abort(lock_table_row_index,current_transaction_row_index, comparing_transaction, row_index_of_comparing_transaction):
    global transaction_table
    global lock_table
    global method
    # transaction_name = str(transaction_name)
    
    if method != "4":
        transaction_table.iloc[row_index_of_comparing_transaction]["State"] = "Aborted"
        transaction_table.iloc[row_index_of_comparing_transaction]["Blocked-by"] = []
        transaction_table.iloc[row_index_of_comparing_transaction]["Blocked-Operations"] = []
        lock_table.iloc[lock_table_row_index]["T-ID"].remove(comparing_transaction)
        temp = (lock_table["T-ID"].values)[:]
    # Realeasing all the locks held by it
        for i in temp:
            if comparing_transaction in i:
                location = list(temp).index(i)
                (lock_table["T-ID"].values)[location].remove(comparing_transaction)
                if not (lock_table["T-ID"].values)[location]:
                    data_item_row = (list(lock_table["T-ID"].values)).index((lock_table["T-ID"].values)[location])
                    lock_table = lock_table.drop(lock_table.index[data_item_row])
        lock_table = lock_table.reset_index(drop = True)
        if list(transaction_table["Blocked-by"].values):
            for j in transaction_table["Blocked-by"].values:
                if comparing_transaction in j:
                    transaction_row = (list(transaction_table["Blocked-by"].values)).index(j)
                    j.remove(comparing_transaction)
                    transaction_table.iloc[transaction_row]['State'] = "Active"
                    for k in list(transaction_table.iloc[transaction_row]["Blocked-Operations"][:]):
                        main(k)
                    if transaction_table.iloc[transaction_row]["Blocked-by"] == []:
                        transaction_table.iloc[transaction_row]["Blocked-Operations"] = []
    elif method == "4":
        # In this method we abort the current transaction
        transaction_table.iloc[current_transaction_row_index]["State"] = "Aborted"
        transaction_table.iloc[current_transaction_row_index]["Blocked-by"] = []
        transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"] = []
        current_transaction = transaction_table.iloc[current_transaction_row_index]["T-ID"]
        temp = (lock_table["T-ID"].values)[:]
        for i in temp:
            if current_transaction in i:
                location = list(temp).index(i)
                (lock_table["T-ID"].values)[location].remove(current_transaction)
                if not (lock_table["T-ID"].values)[location]:
                    data_item_row = (list(lock_table["T-ID"].values)).index((lock_table["T-ID"].values)[location])
                    lock_table = lock_table.drop(lock_table.index[data_item_row])
        lock_table = lock_table.reset_index(drop = True)
    # Continue the blocked transactions
        if list(transaction_table["Blocked-by"].values):
            for j in transaction_table["Blocked-by"].values:
                if current_transaction in j:
                    transaction_row = (list(transaction_table["Blocked-by"].values)).index(j)
                    j.remove(current_transaction)
                    transaction_table.iloc[transaction_row]['State'] = "Active"
                    for k in list(transaction_table.iloc[transaction_row]["Blocked-Operations"][:]):
                        main(k)
                    if transaction_table.iloc[transaction_row]["Blocked-by"] == []:
                        transaction_table.iloc[transaction_row]["Blocked-Operations"] = []

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
                print("Being a shared lock,","T"+str(name[1]),"also acquires the R Lock for",name[3])
            
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
                            
                            abort(row_index,current_transaction_row_index,lock_holding_transaction,row_index_of_comparing_transaction)
                            print(lock_holding_transaction,"is aborted as it is younger than",transaction)
                        else:
                                
                            transaction_table.iloc[current_transaction_row_index]["State"] = "Blocked"
                            transaction_table.iloc[current_transaction_row_index]["Blocked-by"].append(str(lock_holding_transaction))
                            if name not in transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"]:
                                transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"].append(name)
                            print(transaction,"waits for the older transaction",lock_holding_transaction,"to release the lock for",name[3])
                        
                        # When every transaction is aborted, the current transaction acquires the lock
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
                        # Lock holding transaction gets aborted
                            abort(row_index,current_transaction_row_index,lock_holding_transaction,row_index_of_comparing_transaction)
                            print(lock_holding_transaction,"is aborted as it is older than",transaction)
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
                        abort(row_index,current_transaction_row_index,lock_holding_transaction,row_index_of_comparing_transaction)
                        print(lock_holding_transaction,"is aborted and",transaction,"acquires the R lock for",name[3])
 
                        if not lock_table.iloc[row_index]["T-ID"]:
                            lock_table.iloc[row_index]["Lock-Mode"] = name[0].upper()
                            lock_table.iloc[row_index]["T-ID"].append(transaction)
                elif method == "4":
                    # Cautious-Waiting
                    
                    if transaction in lock_table.iloc[row_index]["T-ID"]:
                        lock_table.iloc[row_index]["Lock-Mode"] = name[0].upper()
                        
                    else:
                        lock_holding_transaction = (lock_table.iloc[row_index]["T-ID"])[0]
                        row_index_of_comparing_transaction = int(transaction_table[transaction_table["T-ID"] == lock_holding_transaction].index[0])
                        state_of_lock_holding_transaction = transaction_table.iloc[row_index_of_comparing_transaction]["State"]
                        # Checks the state of the lock holding transaction
                        if state_of_lock_holding_transaction == "Blocked":
                            abort(row_index,current_transaction_row_index,lock_holding_transaction,row_index_of_comparing_transaction)
                            print(transaction,"is aborted as",lock_holding_transaction,"is blocked.")
                        else:
                            # Wait for the lock lock holding transaction
                            transaction_table.iloc[current_transaction_row_index]["State"] = "Blocked"
                            transaction_table.iloc[current_transaction_row_index]["Blocked-by"].append(str(lock_holding_transaction))
                            if name not in transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"]:
                                transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"].append(name)
                            print(transaction,"waits for",lock_holding_transaction,"to release",name[3])
        else:
            add_row = {"Data-Item": str(name[3]), "Lock-Mode":name[0].upper(), "T-ID": ["T"+str(name[1])]}
            lock_table = lock_table.append(add_row, ignore_index=True)
            print("T"+str(name[1]),"acquires the R lock for", name[3])

    # Transaction is blocked, thus operation is appended to list of blocked-operations            
    if transaction_table.iloc[int(name[1])-1]["State"] == "Blocked" and name not in transaction_table.iloc[int(name[1])-1]["Blocked-Operations"]:
        row_index = int(transaction_table[transaction_table["T-ID"] == "T"+str(name[1])].index[0])
        transaction_table.iloc[row_index]["Blocked-Operations"].append(name)
        print("As", "T"+str(name[1]), "is blocked", name, "is appended to the list of blocked-operations.")
        
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
                        print(transaction, "upgrades R lock to W lock for", name[3])
                    else:
                        for i in list(lock_table.iloc[row_index]["T-ID"]):
                            row_index_of_comparing_transaction = int(transaction_table[transaction_table["T-ID"] == i].index[0])
                            timestamp_of_comparing_transaction = transaction_table.iloc[row_index_of_comparing_transaction]["TimeStamp"]
                            if transaction_timestamp < timestamp_of_comparing_transaction:
                            # i gets aborted
                                abort(row_index,current_transaction_row_index,i,row_index_of_comparing_transaction)
                                print(i,"is aborted as it is younger than",transaction)
                                
                            else:
                            # current transaction waits
                            
                                transaction_table.iloc[current_transaction_row_index]["State"] = "Blocked"
                                if i not in transaction_table.iloc[current_transaction_row_index]["Blocked-by"]:
                                    transaction_table.iloc[current_transaction_row_index]["Blocked-by"].append(str(i))
                                if name not in transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"]:
                                    transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"].append(name)
                                print(transaction,"is blocked by",i,"for",name[3])
                        if not lock_table.iloc[row_index]["T-ID"]:
                            lock_table.iloc[row_index]["Lock-Mode"] = name[0].upper()
                            lock_table.iloc[row_index]["T-ID"].append(transaction)
                            print(transaction,"acquires the W lock for",name[3])
                        else:
                            lock_table.iloc[row_index]["T-ID"].append(transaction)
                
                else:
                    # When lock requesting transaction is not present in the lock holding T-IDs
                    for i in list(lock_table.iloc[row_index]["T-ID"]):
                        row_index_of_comparing_transaction = int(transaction_table[transaction_table["T-ID"] == i].index[0])
                        timestamp_of_comparing_transaction = transaction_table.iloc[row_index_of_comparing_transaction]["TimeStamp"]
                        if transaction_timestamp < timestamp_of_comparing_transaction:
                            # i gets aborted
                            abort(row_index,current_transaction_row_index,i,row_index_of_comparing_transaction)
                            print(i,"is aborted as it is younger than",transaction)
                            
                        else:
                            # current transaction waits
                            transaction_table.iloc[current_transaction_row_index]["State"] = "Blocked"
                            if i not in transaction_table.iloc[current_transaction_row_index]["Blocked-by"]:
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
                        for i in list(lock_table.iloc[row_index]["T-ID"]):
                            row_index_of_comparing_transaction = int(transaction_table[transaction_table["T-ID"] == i].index[0])
                            timestamp_of_comparing_transaction = transaction_table.iloc[row_index_of_comparing_transaction]["TimeStamp"]
                            if transaction_timestamp > timestamp_of_comparing_transaction:
                            # i gets aborted
                                abort(row_index,current_transaction_row_index,i,row_index_of_comparing_transaction)
                                print(i,"is aborted as it is older than",transaction)
                                
                            else:
                            # current transaction waits
                            
                                transaction_table.iloc[current_transaction_row_index]["State"] = "Blocked"
                                if i not in transaction_table.iloc[current_transaction_row_index]["Blocked-by"]:
                                    transaction_table.iloc[current_transaction_row_index]["Blocked-by"].append(str(i))
                                if name not in transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"]:
                                    transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"].append(name)
                                print(transaction,"is blocked by",i,"for",name[3])
                        if not lock_table.iloc[row_index]["T-ID"]:
                            lock_table.iloc[row_index]["Lock-Mode"] = name[0].upper()
                            lock_table.iloc[row_index]["T-ID"].append(transaction)
                            print(transaction,"acquires the W lock for",name[3])
                        else:
                            lock_table.iloc[row_index]["T-ID"].append(transaction)
                
                else:
                    for i in list(lock_table.iloc[row_index]["T-ID"]):
                        row_index_of_comparing_transaction = int(transaction_table[transaction_table["T-ID"] == i].index[0])
                        timestamp_of_comparing_transaction = transaction_table.iloc[row_index_of_comparing_transaction]["TimeStamp"]
                        if transaction_timestamp > timestamp_of_comparing_transaction:
                            # i gets aborted
                           abort(row_index,current_transaction_row_index,i,row_index_of_comparing_transaction)
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
                        abort(row_index,current_transaction_row_index,i,row_index_of_comparing_transaction)
                lock_table.iloc[row_index]["Lock-Mode"] = name[0].upper()
            elif method == "4":
                if transaction in lock_table.iloc[row_index]["T-ID"]  and len(list(lock_table.iloc[row_index]["T-ID"])) == 1:
                    lock_table.iloc[row_index]["Lock-Mode"] = name[0].upper()
                    print(transaction,"upgrades to W lock on",name[3])

                else:
                    for i in list(lock_table.iloc[row_index]["T-ID"]):
                        if i != transaction:
                            row_index_of_comparing_transaction = int(transaction_table[transaction_table["T-ID"] == i].index[0])
                            status_of_current_transaction = transaction_table.iloc[row_index]["State"]
                            status_of_comparing_transaction = transaction_table.iloc[row_index_of_comparing_transaction]["State"]
                            if status_of_comparing_transaction == "Blocked" and status_of_current_transaction != "Aborted":
                                print(transaction,"is aborted and locks are released as",i,"is blocked.")
                                abort(row_index,current_transaction_row_index,i,row_index_of_comparing_transaction)
                                

                            elif status_of_comparing_transaction == "Active" and status_of_current_transaction != "Aborted":
                                transaction_table.iloc[current_transaction_row_index]["State"] = "Blocked"
                                if i not in transaction_table.iloc[current_transaction_row_index]["Blocked-by"]:
                                    transaction_table.iloc[current_transaction_row_index]["Blocked-by"].append(str(i))
                                if name not in transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"]:
                                    transaction_table.iloc[current_transaction_row_index]["Blocked-Operations"].append(name)
                                print(transaction,"waits for",i,"to release",name[3])
        else:
            add_row = {"Data-Item": str(name[3]), "Lock-Mode":name[0].upper(), "T-ID": ["T"+str(name[1])]}
            lock_table = lock_table.append(add_row, ignore_index=True)
            
    if transaction_table.iloc[int(name[1])-1]["State"] == "Blocked" and name not in transaction_table.iloc[int(name[1])-1]["Blocked-Operations"]:
        row_index = int(transaction_table[transaction_table["T-ID"] == "T"+str(name[1])].index[0])
        transaction_table.iloc[row_index]["Blocked-Operations"].append(name)
        print("As","T"+str(name[1]), "is blocked operation is added to the list to Blocked-Operations.")
    
    if transaction_table.iloc[int(name[1])-1]["State"] == "Aborted" and method != "4":
        print("T"+str(name[1]),"is already aborted. So no changes in the tables.")
            
def end_transaction(name):
    global transaction_table
    global lock_table 
    
    transaction = "T"+str(name[1])
    if transaction_table.iloc[int(name[1])-1]["State"] == "Active":
        #Status change to committed
        print(transaction,"committed successfully and respective locks are released.")
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



if __name__ == "__main__":
    file = "input-1.txt"
    print("Current file:",file)
    transaction_table = pd.DataFrame(columns=["T-ID", "TimeStamp","State", "Blocked-by", "Blocked-Operations"])
    lock_table = pd.DataFrame(columns=["Data-Item","Lock-Mode","T-ID"])
    proceed = True
    while proceed:
        method = input("Please select the method of concurrency control to implement:\n1. Wound & Wait\n2. Wait & Die\n3. No-Waiting\n4. Cautious Waiting\nSelect 1,2,3 or 4.\nType here: ")
        print("You selected",method,"\n\n\n\n")
        if method == "1":
            read_input(file)
            proceed = False
        elif method == "2":
            read_input(file)
            proceed = False
        elif method == "3":
            read_input(file)
            proceed = False
        elif method == "4":
            read_input(file)
            proceed = False
        else:
            print("\n\n******Please select again.")
        

        

