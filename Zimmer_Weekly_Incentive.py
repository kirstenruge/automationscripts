__author__ = 'kruge'

from RB5QATools import (qahelper as qh,
                        qathreading as qt,
                        reporter as rep,
                        Tools as qa,
                        API as a,
                        fileloader as fl,
                        tools_populations as tpop,
                        qaincentive as qi,
                        tools_plan as tp,
                        tools_identity as tid,
                        tools_incentive as ti)
import threading

debug = rep.Reporter().logger.debug

orgid = 56
file_population = 'zimmer_enrolled_01-latest'
delivery_tag = ['zimmer_dm_2017:dollar', 'zimmer_200_cap']

env = 'prod'

IncentiveQA = qh.QAHelper(orgid=orgid,
                          filetype='incentive',
                          test=True,
                          headerindicator='H',
                          trailerindicator='T')
IncentiveFile = IncentiveQA.File
QALog = IncentiveQA.Reporter
file_date = IncentiveFile.FileDate





lock = threading.Lock()
p = a.APIWorker(env=env, scriptname=__file__)
datadict = {}
#file_relationships = tpop.GetPopRelationships(popname=file_population,
 #                                             orgid=orgid,
  #                                            apiobject=p)
consumerlist = tpop.GetPopConsumers(popname=file_population, orgid=orgid)
file_format = ["RBH CLIENT ID", "RBH CLIENT NAME", "EMPLOYEE RBH PERSON ID", "EMPLOYEE ID", "EMPLOYEE SSN", "EMPLOYEE FIRST NAME",
               "EMPLOYEE LAST NAME", "EMPLOYEE DOB", "EMPLOYEE GENDER", "PARTICIPANT RBH PERSON ID", "PARTICIPANT SSN",
               "PARTICIPANT FIRST NAME", "PARTICIPANT LAST NAME", "PARTICIPANT DOB", "PARTICIPANT GENDER", "PARTICIPANT RELATIONSHIP",
               "INCENTIVE ACTIVITY", "INCENTIVE TYPE", "INCENTIVE VALUE", "ACTIVITY COMPLETION DATE",
               "ACTIVITY INCENTIVE EARNED DATE"]

fileconsumerlist = []
class FileVerification():
    def __init__(self):
        self.column_names = False
        self.header = False
        self.trailerCount = 0
        self.trailerFlag = False
        self.consumeridlist = []
        self.spouselist = []
        QALog.Testcase(testcase='FV2')
        qa.StartTimer("{0} - Validating File Structure".format(QALog.testcase))
        for row in IncentiveFile.FileReader:
            self.FV2(row)
        for key, value in datadict.iteritems():
            rosterid = tid.GetRosterData(consumerid=key)

        self.Cleanup()


        QALog.GetPassFail(recordcount=IncentiveFile.FileUniqueCount, threshold=0, thresholdtype='absolute')
        QALog.PrintErrors()

    def FV2(self, row):
        # Check date in header row
        if row[0] == 'H':
            if row[1].upper() != 'REDBRICK':
                QALog.AddError("Invalid sender", row[1])
            if qa.ConvDate(row[2]) != IncentiveFile.FileDate:
                QALog.AddNotification("Header date does not match file date")
            return

        # Get record count in the trailer line
        if row[0] == 'T':
            self.trailerCount = qa.GetInt(row[2])
            self.signature = row[1]
            if self.signature != "RedBrick Health":
                QALog.AddError(self, "Signature in trailer row missing", self.signature)
            self.trailerFlag = True
            return
        if len(row) != len(file_format):
            QALog.AddError("Invalid record format: {0}".format("".join(row)))
            return

        QALog.CountOne()
        qa.RunTimer("{0} - Validating File Structure".format(QALog.testcase),
                    QALog.CurrentRecordCount(),
                    IncentiveFile.FileUniqueCount,
                    QALog.GetCounts(type='errors'))


        fields = "RBH CLIENT ID, RBH CLIENT NAME, EMPLOYEE RBH PERSON ID, EMPLOYEE ID, EMPLOYEE SSN, EMPLOYEE FIRST NAME," \
        "EMPLOYEE LAST NAME, EMPLOYEE DOB, EMPLOYEE GENDER, PARTICIPANT RBH PERSON ID, PARTICIPANT SSN," \
        "PARTICIPANT FIRST NAME, PARTICIPANT LAST NAME, PARTICIPANT DOB, PARTICIPANT GENDER, PARTICIPANT RELATIONSHIP," \
        "INCENTIVE ACTIVITY, INCENTIVE TYPE, INCENTIVE VALUE, ACTIVITY COMPLETION DATE," \
        "ACTIVITY INCENTIVE EARNED DATE,"
        fielddict = {'RBH CLIENT ID': row[0], 'RBH CLIENT NAME': row[1], 'EMPLOYEE RBH PERSON ID': row[2], 'EMPLOYEE ID': row[3], 'EMPLOYEE SSN': row[4], 'EMPLOYEE FIRST NAME': row[5], 'EMPLOYEE LAST NAME': row[6],
                     'EMPLOYEE DOB': row[7], 'EMPLOYEE GENDER': row[8], 'PARTICIPANT RBH PERSON ID': row[9], 'PARTICIPANT SSN': row[10],
                     'PARTICIPANT FIRST NAME': row[11], 'PARTICIPANT LAST NAME': row[12], 'PARTICIPANT DOB': row[13], 'PARTICIPANT GENDER': row[14], 'PARTICIPANT RELATIONSHIP': row[15],
                     'INCENTIVE ACTIVITY': row[16], 'INCENTIVE TYPE': row[17], 'INCENTIVE VALUE': row[18], 'ACTIVITY COMPLETION DATE': row[19],
                     'ACTIVITY INCENTIVE EARNED DATE': row[20]}
       ###this is a dictionary of consumerid to incentive value
        fileEmployeeID = fielddict['PARTICIPANT RBH PERSON ID']
        consumerid = tid.GetConsumerID(rosterid=fileEmployeeID, orgid=orgid)
        fileconsumerlist.append(consumerid)
        datadict[consumerid] = row[18]

        for i in range(0, 20, 1):
            if row[i] is None:
                if i != 16:
                    QALog.AddError(self, "Values cannot be null", fielddict.keys()[fielddict.values().index(i)])

                    print("Values cannot be null" + str(fielddict.keys()[fielddict.values().index(i)]))




    # Set rosterId for logging errors


        if consumerid in self.consumeridlist:
            QALog.AddError(self, "Duplicate entries for consumer", consumerid)
            print("Duplicate entries for consumer id " + str(consumerid))
        else:
            self.consumeridlist.append(consumerid)
        print("consumer # " + str(consumerid))





    def Cleanup(self):
        # Validates trailer count vs actual count; this probably will never change
        QALog.SetVars(consumerid=None, consrosterid=None)
        if self.trailerFlag and IncentiveFile.FileUniqueCount != self.trailerCount:
            QALog.AddError("Trailer count does not match record count")
       # if not file_relationships:
        #    QALog.AddError("File population is empty")
        print("datadict has " + str(len(datadict)) + " entries")
        print("consumers has " + str(len(consumerlist)) + " peeps in it")



class DataVerification():
    def __init__(self):
        # This is standard unless the file is NOT a regular RB5 delta file
        TxnIDInfo = ti.GetTransactionHistory(orgid=orgid,
                                             beforedate=IncentiveFile.FileDate,
                                             deliverytag=delivery_tag)

        self.startDate = TxnIDInfo[max(TxnIDInfo)].start_date
        self.endDate = TxnIDInfo[max(TxnIDInfo)].end_date
        self.txnStart = TxnIDInfo[max(TxnIDInfo)].txn_start
        self.txnEnd = TxnIDInfo[max(TxnIDInfo)].txn_end
        print "Start: {0}".format(self.startDate)
        print "End: {0}".format(self.endDate)
        print "TxnStart: {0}".format(self.txnStart)
        print "TxnEnd: {0}".format(self.txnEnd)


        self.employeeidlist = []
        QALog.Testcase(testcase='DV1')
        qa.StartTimer("{0} - Validating File Contents".format(QALog.testcase))


        qt.ThreadedQA(24, self.DV1, QALog.testcase, datadict)

        QALog.GetPassFail(recordcount=IncentiveFile.FileUniqueCount, threshold=.2, thresholdtype='%')
        QALog.PrintErrors()



        QALog.GetPassFail(recordcount=IncentiveFile.FileUniqueCount, threshold=.2, thresholdtype='%')
        QALog.PrintErrors()

        QALog.Testcase(testcase='DV2')
        qa.StartTimer("{0} - Validating Full Population".format(QALog.testcase))

        qt.ThreadedQA(16, self.DV2, QALog.testcase, datadict)

        QALog.GetPassFail(recordcount=len(consumerlist), threshold=.2, thresholdtype='%')
        QALog.PrintErrors()


    def keys_of_value(dct, value):
        for k in dct:
            if isinstance(dct[k], list):
                if value in dct[k]:
                    return k
        else:
            if value == dct[k]:
                return k
    def DV1(self, t, consid):
        # Add 1 to checked record count
        QALog.CountOne()
        fileconsumerid = consid
        fileIncentive = datadict[consid]
        print("consumer id : " + str(fileconsumerid) + " , " + str(fileIncentive))

        # Run timer
        qa.RunTimer("{0} - Validating File Contents".format(QALog.testcase),
                    QALog.CurrentRecordCount(),
                    IncentiveFile.FileUniqueCount,
                    QALog.GetCounts(type='errors'))

        # Get incentive file record from dictionary created in FV2

        # Pull fields from that file record
        # This part here will change based on what is actually in the file


        emp_rcuTotal = 0.00
        spouse_rcuTotal = 0.00
        emp_achievedTotal = 0.00
        spouse_achievedTotal = 0.00
        emp_expectedTotal = 0.00

        rcuTotal = 0.00
        expectedTotal = 0.00
        achievedTotal = 0.00


            # Check consumer's award status
        cas = qi.IncentiveQA(apiobject=p).ConsumerAwardStatus(consumerid=fileconsumerid,
                                                                  fromdate=self.startDate,
                                                                  todate=self.endDate,
                                                                  deliverytag=delivery_tag

                                                                 )
        rcu_errors = ti.qaRCUs(consumerid=consid,
                                   apiobject=p)
        considd = cas[0]['consumerid']
        if considd != fileconsumerid:
            print("Consumer id from function is " + str(considd) + ", and your value is " + str(fileconsumerid))
           # Get RCU and expected records from consumer award status result
        cons_rcuRecord = cas[11]
        cons_expectedRecord = cas[7]
        cons_achievedRecord = cas[10]
        converted = cas[8]
            # Get totals from RCU and expected records
        cons_rcuTotal = qa.calctotal(cons_rcuRecord, self.txnStart, self.txnEnd)
        cons_achievedTotal = qa.calctotal(cons_achievedRecord, self.startDate, self.endDate)
        cons_expectedTotal = cons_expectedRecord.get('total', 0)

        fileachieved = fileIncentive
        rcuTotal += cons_rcuTotal
        expectedTotal += cons_expectedTotal
        achievedTotal += cons_achievedTotal
        floatachievedTotal = float(rcuTotal)
        intfilevalue = float(fileachieved)
        if floatachievedTotal != intfilevalue:
            QALog.AddError("Converted value {0} does not match file {1}".format(rcuTotal, fileIncentive))
            print("System has {0} value, file has {1}".format(achievedTotal, fileIncentive))
            print("Consumer id from function is " + str(considd) + ", and your value is " + str(fileconsumerid))
        elif int(expectedTotal) != int(rcuTotal):
            QALog.AddNotification("Expected total {0} does not match achieved total {1}".format(expectedTotal, achievedTotal))
        else:
            print("okay")


        # Acquire lock for writing errors to database
        lock.acquire()
        # Set consumerid/rosterid for logging errors
        QALog.SetVars(consumerid=fileconsumerid)




        # Release lock
        lock.release()

    def DV2(self, t, consid):
        QALog.CountOne()

        # Run timer
        qa.RunTimer("{0} - Validating File Contents".format(QALog.testcase),
                    QALog.CurrentRecordCount(),
                    len(file_relationships),
                    QALog.GetCounts(type='errors'))

        if consid in IncentiveFile.DataDict:
            return

        # Check consumer's award status
        cas = qi.IncentiveQA(apiobject=p).ConsumerAwardStatus(consumerid=consid,
                                                              fromdate=self.startDate,
                                                              todate=self.endDate,
                                                              deliverytag=delivery_tag)
        rcu_errors = ti.qaRCUs(consumerid=consid,
                               apiobject=p)

        # Get consumerid from consumer award status result
        consid = cas[0]['consumerid']

        rcuRecord = cas[11]
        achievedRecord = cas[10]
        expectedRecord = cas[7]

        # Get totals from RCU and expected records
        rcuTotal = qa.calctotal(rcuRecord, self.startDate, self.txnEnd)
        txn_rcuTotal = qa.calctotal(rcuRecord, self.txnStart, self.txnEnd)
        achievedTotal = qa.calctotal(achievedRecord, self.startDate, self.endDate)
        expectedTotal = expectedRecord.get('total', 0)

        # Acquire lock for writing errors to database
        lock.acquire()
        # Set consumerid/rosterid for logging errors
        QALog.SetVars(consumerid=consid)

        for error_message in rcu_errors:
            QALog.AddError(error_message)

        if rcuTotal < achievedTotal:
            stuck_total = achievedTotal - rcuTotal
            QALog.AddError("Consumer has {0} points stuck in achieved".format(stuck_total))

        if txn_rcuTotal > 0:
            QALog.AddError("Consumer not on file and has converted RCUs {0} for current period".format(txn_rcuTotal))

        if achievedTotal != rcuTotal:
            QALog.AddError("Consumer's achieved RCUs {0} do not match RCU total {1}".format(achievedTotal, rcuTotal))
        if rcuTotal != expectedTotal:
            QALog.AddWarning("RCU total {0} does not match expected total {1}".format(rcuTotal, expectedTotal))

        lock.release()


FileVerification()
DataVerification()
IncentiveQA.EndQA()
