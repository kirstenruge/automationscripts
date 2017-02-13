import threading
import datetime

from RB5QATools import qathreading as qt
from RB5QATools import qaincentive as qi
from RB5QATools import reporter as rep
from RB5QATools import Tools as qa
from RB5QATools import API as a
from RB5QATools import qahelper as qh
from RB5QATools import dbhelper as db
from RB5QATools import stage_tools as st
from RB5QATools import tools_incentive as ti
from RB5QATools import tools_populations as tpop
from RB5QATools import tools_plan as tp
from RB5QATools import tools_identity as tid

__author__ = 'kruge'
env = 'prod'

orgid = 65
wagetype = 1757
filetype = 'incentive'
reconciliation_population = ['BMS_Non_Consumer_Choice_EE']
file_population = ['BMS_Non_Consumer_Choice_EE']
delivery_tag = 'bms_2017_dm:weekly_dollars_points'
poplist = []
consumerlist = []
consumerdict = {}
consumerSumDict = {}
doubleHADict = {}
dvlist = []
consumeridlist = []

IncentiveQA = qh.QAHelper(orgid=orgid,
                          filetype='incentive',
                          test=True,
                          headerindicator='H',
                          trailerindicator='T')
IncentiveFile = IncentiveQA.File
QALog = IncentiveQA.Reporter
today = datetime.datetime.now()
lock = threading.Lock()

p = a.APIWorker(env=env, scriptname=__file__)

FilePopulation = tpop.GetPopConsumers(orgid=orgid, popname=file_population)

# Spouse Lookup - useful when it's a EE-rollup file
spouseDict = qa.SpouseLookup(orgid=orgid)
filedate = IncentiveFile.FileDate


class FileVerification:
    def __init__(self):
        self.trailerCount = 0
        self.trailerFlag = False

        QALog.Testcase(testcase='FV2')

        qa.StartTimer("{0} - Validating File Structure".format(QALog.testcase))

        for row in IncentiveFile.FileReader:
            self.FV2(row)
        self.Cleanup()

        QALog.GetPassFail(recordcount=IncentiveFile.FileUniqueCount, threshold=0, thresholdtype='absolute')
        QALog.PrintErrors()

    def FV2(self, row):
        # Check for correct column headers in header row
        if str(row[0]) == 'SSN':
            if row[1] != 'Name':
                QALog.AddError("Invalid column header for Name", row[1])
            if row[2] != 'Wage Type':
                QALog.AddError("Invalid column header for Wage Type", row[2])
            if row[3] != 'Amount':
                QALog.AddError("Invalid column header for Amount", row[3])
            if row[4] != 'Number':
                QALog.AddError("Invalid column header for Number", row[4])
            if row[5] != 'Unit':
                QALog.AddError("Invalid column header for Unit", row[5])
            if row[6] != 'Start Date':
                QALog.AddError("Invalid column header for Start Date", row[6])
            if row[7] != 'End Date':
                QALog.AddError("Invalid column header for End Date", row[7])
            if row[8] != 'Cost Center':
                QALog.AddError("Invalid column header for Cost Center", row[8])
            if row[9] != 'Company Code':
                QALog.AddError("Invalid column header for Company Code", row[9])
            if row[10] != 'Reason':
                QALog.AddError("Invalid column header for Reason", row[10])
            if row[11] != 'Payroll Id':
                QALog.AddError("Invalid column header for Payroll Id", row[11])
            if row[12].upper() != 'PERNR':
                QALog.AddError("Invalid column header for PERNR", row[12])
            if row[13].upper() != 'EMPSTAT':
                QALog.AddError("Invalid column header for EMPSTAT", row[13])
            if row[14].upper() != 'ASSIGNSTAT':
                QALog.AddError("Invalid column header for ASSIGNSTAT", row[14])
            if row[15].upper() != 'BMSID':
                QALog.AddError("Invalid column header for BMSID", row[15])

            return

        qa.RunTimer("{0} - Validating File Structure".format(QALog.testcase),
                    QALog.CurrentRecordCount(),
                    IncentiveFile.FileUniqueCount,
                    QALog.GetCounts(type='errors'))

        fields = "SSN, Name, Wage Type, Amount, Number, Unit, Start Date, End Date, Cost Center, " \
                 "Company Code, Reason, Payroll Id, PERNR, EMPSTAT, ASSIGNSTAT, BMSID, "

        fieldmap = dict(((i, col) for i, col in enumerate([c for c in fields.split(", ")])))
        fileRecord = dict(((fieldmap[i], val) for i, val in enumerate(row)))



        # Set rosterId for logging errors
        fileEmployeeID = fileRecord['BMSID']
        if len(fileEmployeeID) < 8:
            fileEmployeeID = str(fileEmployeeID).zfill(8)

        emprostid = db.GetConsumerRosterIDFromEmpID(orgid, fileEmployeeID)
        QALog.SetVars(consrosterid=emprostid)

        # Check if a consumer has multiple records in file

        if emprostid not in consumerlist:
            consumerlist.append(emprostid)
            consumerdict[emprostid] = fileRecord['Amount']

        elif emprostid in consumerlist:
            QALog.AddError("Duplicate consumers", emprostid, fileEmployeeID)
            print("Duplicate consumers found for emp_id " + fileEmployeeID)

        # if emprostid in IncentiveFile.DataDict:
        #     # if len(IncentiveFile.DataDict[fileConsumerRosterID]) > 1:
        #     #     QALog.AddWarning("Consumer has multiple records in file")
        #     n = max(n for n in IncentiveFile.DataDict[emprostid])
        #     IncentiveFile.DataDict[emprostid][n + 1] = fileRecord
        #
        # IncentiveFile.DataDict[emprostid] = {0: fileRecord}

        try:
            consumerSumDict[emprostid] += float(fileRecord["Amount"])
        except KeyError:
            consumerSumDict[emprostid] = float(fileRecord["Amount"])

        requiredFields = ['Wage Type', 'Amount', 'Start Date', 'End Date', 'BMSID']
        nullFields = ['SSN', 'Name', 'Number', 'Unit', 'Cost Center', 'Company Code', 'Reason', 'Payroll Id', 'PERNR',
                      'EMPSTAT', 'ASSIGNSTAT']
        #
        # # Field/column validation starts here
        if fileRecord['Wage Type'] != str(wagetype):
            QALog.AddError("Invalid Wage Type", fileRecord['Wage Type'], wagetype)
        if fileRecord['Amount'] != '100':
            QALog.AddError("Invalid Amount", fileRecord['Amount'], '100.00')

        for field in fileRecord:
            if field in requiredFields:
                if not fileRecord[field]:
                    QALog.AddError("{0} field is null".format(field))
        for field in nullFields:
            if fileRecord[field]:
                QALog.AddError("{0} field must be null".format(field))
                print("{0} field must be null".format(field))

    def Cleanup(self):
        # Validates trailer count vs actual count; this probably will never change
        QALog.SetVars(consumerid=None, consrosterid=None)
        if self.trailerFlag and IncentiveFile.FileTotalCount != self.trailerCount:
            QALog.AddError("Trailer count does not match record count", self.trailerCount,
                           IncentiveFile.FileUniqueCount)


class DataVerif:
    consumeridlist = []
    def __init__(self):
        # This is standard unless the file is NOT a regular RB5 delta file
        mydate = '2017-02-06'
        TxnIDInfo = p.GetTransactionHistory(orgid=orgid, beforedate=mydate, deliverytag=delivery_tag)

        newestTxnInfo = TxnIDInfo[3]

        self.dateRange = TxnIDInfo[0]
        self.startDate = qa.ConvDateTime(newestTxnInfo[0])  # qa.ConvDateTime('2015-12-18')  #
        self.endDate = qa.ConvDateTime(newestTxnInfo[1])  # qa.ConvDateTime('2016-12-19')  #
        self.txnStart = qa.ConvDateTime(min(self.dateRange)) + datetime.timedelta(seconds=1)
        self.txnEnd = qa.ConvDateTime(max(self.dateRange))

        print "Start: {0}".format(self.startDate)
        print "End: {0}".format(self.endDate)
        print "txnStart: {0}".format(self.txnStart)
        print "txnEnd: {0}".format(self.txnEnd)


        QALog.Testcase(testcase='DV1')
        qa.StartTimer("{0} - Validating File Contents".format(QALog.testcase))
        i = 0
        planid = 0
        for consumer in consumerlist:

            i = tid.GetConsumerID(rosterid=consumer, orgid=65)
            consumeridlist.append(i)
            rosterid = consumer
            planid = tp.GetConsumerPlan(consumerid=i)[0]
            cas = qi.IncentiveQA(apiobject=p).ConsumerAwardStatus(consumerid=i,
                                                                  planid=planid,
                                                                  fromdate=self.startDate,
                                                                  todate=self.endDate,
                                                                  )
            rcuRecord = cas[11]
            expectedRecord = cas[7]
            achievedRecord = cas[10]
            rcuTotal = qa.calctotal(rcuRecord, self.txnStart, self.txnEnd)

            achievedTotal = qa.calctotal(achievedRecord, self.startDate, self.endDate)

            filercu = consumerdict[consumer]

            if float(filercu) != float(achievedTotal):
                QALog.AddError("File amount does not equal expected", filercu, achievedTotal, i, rosterid)
                print("Potential overpayment on consumer " + str(i))
            else:
                continue
        QALog.GetPassFail(recordcount=IncentiveFile.FileUniqueCount, threshold=.2, thresholdtype='%')
        QALog.PrintErrors()

        # qa.RunTimer("{0} - Validating File Contents".format(QALog.testcase),
        #             QALog.CurrentRecordCount(),
        #             len(FilePopulation),
        #             QALog.GetCounts(type='errors'))
        # poplist = p.GetPopulationsByOrg(orgid)
        # dummystart = qa.ConvDateTime('2015-12-12')
        # dummyend = qa.ConvDateTime('2016-12-12')
        # for population in poplist:
        #     dvlist.append(p.GetPopConsumers(population))
        # for consumer in dvlist:
        #     if consumer not in consumeridlist:
        #         cas = qi.IncentiveQA(apiobject=p).ConsumerAwardStatus(consumerid=id,
        #                                                               fromdate=dummystart,
        #                                                               todate=dummyend,
        #                                                               txnstart=dummystart,
        #                                                               txnend=dummyend,
        #                                                               )
        #
        #         # Get consumerid from consumer award status result
        #         consid = cas[0]['consumerid']
        #
        #         rcuRecord = cas[2]
        #         expectedRecord = cas[7]
        #         rcuTotal = rcuRecord.get('total', 0)
        #         expectedTotal = expectedRecord.get('total', 0)
        #
        #         # Acquire lock for writing errors to database
        #         # lock.acquire()
        #         # Set consumerid/rosterid for logging errors
        #         QALog.SetVars(consumerid=consid)
        #
        # if rcuTotal > 0:
        #     QALog.AddError("Consumer has RCUs totalling {0} but is not on file".format(rcuTotal))
        # if expectedTotal > 0:
        #     QALog.AddWarning("Consumer has expected RCUs totaling {0} but is not on file".format(expectedTotal))
        # if rcuTotal != expectedTotal:
        #     QALog.AddWarning("RCU total {0} does not match expected total {1}".format(rcuTotal, expectedTotal))
        #

        QALog.Testcase(testcase='DV2')

        # Run timer






        qt.ThreadedQA(16, self.DV2, QALog.testcase, FilePopulation)

        QALog.GetPassFail(recordcount=IncentiveFile.FileUniqueCount, threshold=.2, thresholdtype='%')
        QALog.PrintErrors()

    ##threading function does the looping for you, take it out
    def DV2(self, t, id):

        QALog.CountOne()
        qa.RunTimer("{0} - Validating File Contents".format(QALog.testcase),
                    QALog.CurrentRecordCount(),
                    len(FilePopulation),
                    QALog.GetCounts(type='errors'))

        if id not in consumeridlist:
            cas = qi.IncentiveQA(apiobject=p).ConsumerAwardStatus(consumerid=id,
                                                                  fromdate=self.startDate,
                                                                  todate=self.endDate,
                                                                  txnstart=self.txnStart,
                                                                  txnend=self.txnEnd,
                                                                  )

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
            lock.acquire()
            QALog.SetVars(consumerid=consid)

            if rcuTotal > 0:

                QALog.AddError("Consumer has RCUs totalling {0} but is not on file".format(rcuTotal))
                print("Consumer ID: " + str(consid) + " has RCUs totalling {0} but is not on file".format(rcuTotal))
                rosterid = tid.GetRosterData(consid)
                print("Consumer info: " + str(rosterid))
            if expectedTotal > 0:
                QALog.AddWarning("Consumer has expected RCUs totaling {0} but is not on file".format(expectedTotal))
                print(
                "Consumer ID: " + str(consid) + " has expected RCUs totalling {0} but is not on file".format(rcuTotal))
            if rcuTotal != expectedTotal:
                QALog.AddWarning("RCU total {0} does not match expected total {1}".format(rcuTotal, expectedTotal))
                print("Consumer ID: " + str(consid) + " has expected RCUs totalling {0} but does not match file".format(
                    rcuTotal))
            lock.release()


FileVerification()
DataVerif()
IncentiveQA.EndQA()
