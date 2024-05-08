import pandas as pd
import numpy as np 
import datetime
import random 
# 載入數據
agv_count = int(input("input your agv count."))
data_agv = pd.read_excel(r"<put your document path here>", sheet_name="2310_AGV0102")
data_distance = pd.read_excel(r"<put your document path here>", sheet_name="P to P_Distance")
MachineWeight = pd.read_excel(r"<put your document path here>", sheet_name="Weight") 
data = data_agv 


data.index = [i+1 for i in data.index]
print("im data")
print(data)
Scheduling_type = str(input("input your schedualing type 1.FCFS or 2.distance 3.random")).lower()
typedict = {"1":"fcfs","2":"distance","3":"random"}
UseWeight = True if str(input("Use Weight or not? 1.Yes,2.No")) == "1" else False
Scheduling_type = typedict[Scheduling_type]
def timecounter(functionName):
    def outer(func):
        def inner():
            startTime = datetime.datetime.now()
            func()
            endTime = datetime.datetime.now()
            UseTime = endTime-startTime
            print(f"{functionName}:",end='')
            print(f"Use time {UseTime}")
            print(Scheduling_type)
        return inner
    return outer


def GetOverlapIndex(data):
    
    # Convert 'JOB_END_TIME' and 'LOT_END_TIME' to datetime objects
    data['END_TIME'] = pd.to_datetime(data['END_TIME'], errors='coerce')
    data['Start_TIME'] = pd.to_datetime(data['Start_TIME'], errors='coerce')
    data['JOB_ASSIGN_TIME'] = pd.to_datetime(data['JOB_ASSIGN_TIME'], errors='coerce')
    single_test_waittingtime = 0 
    sechdualingEndtime = 0
    sechdualjobcount = 0 
    waitingSechdualingJob = []
    finalseq = []
    temp = [] 
    totalWaittingTime = 0 
    count = 0

    for index, current_job in data.iterrows():
        
        concurrent_index = []
        data = data.iloc[:]
        agvservicejobs = data.iloc[index:index+agv_count]

        current_job_endTime = current_job["END_TIME"]
        
        # print(agvservicejobs[])
        if index not in temp:
            temp.append(index)
            for index,activatejob in agvservicejobs.iterrows():

                overlaps = data[(data['Start_TIME'] > activatejob['Start_TIME']) & (data['Start_TIME'] < activatejob['END_TIME'])]

                    
            if len(overlaps)>agv_count :  
 
                waitingTime,sechdualSeq,finishTime = SingleSchedualing(index,overlaps)
                finalseq.append(sechdualSeq)
                
                single_test_waittingtime+=waitingTime


                
                totalWaittingTime += waitingTime
                preschedualingidx = sechdualSeq
                
                timedeltaFinishtime = float(finishTime)
                timedeltaFinishtime = datetime.timedelta(seconds=timedeltaFinishtime)
                sechdualingEndtime = current_job_endTime+timedeltaFinishtime
                waitingSechdualingJob = data[(data['Start_TIME']>current_job_endTime) & (data['Start_TIME'] < sechdualingEndtime)]
                sechdualjobcount = len(waitingSechdualingJob.index.values.tolist())

                temp.extend(sechdualSeq)
                inthetemp = False 
                for idx in waitingSechdualingJob.index.values.tolist():
                    if idx in temp :
                        inthetemp = True
                if sechdualjobcount>0 and not inthetemp:
                    wattingtime,seq,temprecursive = doublesechdualingRecursive(preschedualingidx,waitingSechdualingJob,sechdualjobcount,sechdualingEndtime,totalWaittingTime)
                    temprecursive = flatten(temprecursive)
                    temp.extend(flatten(seq))
                    finalseq.extend(seq)
                    totalWaittingTime=wattingtime

    return finalseq,totalWaittingTime


def flatten(lst):
    result = []
    for item in lst:
        if isinstance(item, list):
            result.extend(flatten(item))
        else:
            result.append(item)
    return result
def doublesechdualingRecursive(preschedualingidx,waitingSechdualingJob,sechdualjobcount,presechdualingEndtime,totalWaittingTime):
    temp = [] 
    totalSchdualing = [] 
    lastschedualing = preschedualingidx[-1]
    waitingTime,sechdualSeq,finishTime = multisechedualing(lastschedualing,waitingSechdualingJob)

    totalWaittingTime += float(waitingTime)
    preschedualingidx = sechdualSeq
    timedeltaFinishtime = float(finishTime)
    timedeltaFinishtime = datetime.timedelta(seconds=timedeltaFinishtime)

    sechdualingEndtime = presechdualingEndtime+timedeltaFinishtime

    # 這邊會遇到重複的double 
    # 需要濾掉已經run 過的double
    waitingSechdualingJob = data[(data['Start_TIME']>presechdualingEndtime) & (data['Start_TIME'] < sechdualingEndtime)]

    sechdualjobcount = len(waitingSechdualingJob.index.values.tolist())

    temp.extend(sechdualSeq)
    totalSchdualing.append(sechdualSeq)
    if sechdualjobcount>0:
        wattingtime,seq,temp = doublesechdualingRecursive(preschedualingidx,waitingSechdualingJob,sechdualjobcount,sechdualingEndtime,totalWaittingTime)
        totalSchdualing.extend(seq)
        return wattingtime,totalSchdualing,temp
    else:
        return totalWaittingTime,totalSchdualing,temp
def SingleSchedualing(index,overlaps):

    overlapIdx = overlaps.index.values.tolist()
    overlapIdx.insert(0,index)
    
    action_dict = GetOverlapMachineID(overlapIdx)
    SechedualList = DistanceSchedualing(overlapIdx,action_dict)
    print(f"scheduale Sequence: {SechedualList}")

    # print(f"Sechedualist : {SechedualList}")
    waitingTime,_,finishTime = DistanceWaitingCalculate(SechedualList,action_dict)

    return waitingTime,SechedualList,finishTime

def multisechedualing(preschedualingidx,waitingSechdualingJob):

    overlapIdx = waitingSechdualingJob.index.values.tolist()


    overlapIdx.insert(0,preschedualingidx)
    action_dict = GetOverlapMachineID(overlapIdx)
    SechedualList = DistanceSchedualing(overlapIdx,action_dict)
    print(f"schedualing Sequence: {SechedualList}")
    
    # print(f"Sechedualist : {SechedualList}")
    waitingTime,_,finishTime = DistanceWaitingCalculate(SechedualList,action_dict)

    return waitingTime,SechedualList,finishTime
            

def GetFCFSIndex(data):

    non_overlap_index = [] 

    for index, current_job in data.iterrows():
             non_overlap_index.append(index)
    return non_overlap_index

def GetNonOverlapIndex(overlap_index:list):
    flatten_overlap_index = []
    non_overlap_index = [] 
    
    for idxs in overlap_index:
        flatten_overlap_index.extend(idxs[1:])
    for index, current_job in data.iterrows():
        if index not in flatten_overlap_index:
             non_overlap_index.append(index)
    return non_overlap_index
def GetNonoverlapIndexWattingTime(NonOverlapindex):
    totalsec = 0 
    action_dict = {}
    total_distance = 0 
    dataDistance_tmp = data_distance.set_index("TESTER_ID",inplace=False,drop=True)

    EL_list = ['T1-ES-01', 'T1-ES-02', 'T1-ES-03', 'T1-ES-04', 'T1-ES-05', 'T1-ES-06', 'T1-ES-07', 'T1-ES-08', 'T1-ES-09','AGV1']
    for idx in NonOverlapindex:
        # print(pd.to_timedelta(f"{data.loc[idx,'END_TIME']}"))
        # if idx in data.index:
        tester_id = data.loc[idx, 'TESTER_ID']
        tran_type = data.loc[idx, 'TRANS_TYPE']
        # use for distance calculate
        if str(tran_type).upper() == "SWAP":
            action_dict[idx] = {tester_id:[tester_id,data.loc[idx, 'ERACK_ID_OUT'],data.loc[idx, 'ERACK_ID_IN'],tester_id]}
        elif str(tran_type).upper() == "LOAD":
            action_dict[idx] = {tester_id:[data.loc[idx, 'ERACK_ID_IN'],tester_id]}
        elif str(tran_type).upper() == "UNLOAD":
            action_dict[idx] = {tester_id:[tester_id,data.loc[idx, 'ERACK_ID_OUT']]}
        else: 
            action_dict[idx] = {tester_id:[]}


    for idx in NonOverlapindex:
        preID = "AGV1"
        if True:
            for position in action_dict[idx][list(action_dict[idx].keys())[0]]:
                # print(str(preID),position)
                if not np.isnan(dataDistance_tmp.loc[position,str(preID)]):

                    total_distance += dataDistance_tmp.loc[position,str(preID)]
                else: 
                    # print(idx)
                    if  not np.isnan(dataDistance_tmp.loc[str(preID),position]):
                        total_distance += dataDistance_tmp.loc[str(preID),position]
                    else: 
                        nan_count+=1
                preID = position
    total_waitting_time = total_distance/0.6   


    return total_waitting_time 

def GetOverlapMachineID(overlap_indice):
    
    # Iterate over the tester_ids_for_first_indices list

    if 'TESTER_ID' not in data_distance.columns:
        raise ValueError("Column 'TESTER_ID' not found in the 'Total Distance' sheet.")

    # Correcting the KeyError issue
    tester_id_to_agv01a_distance = {}
    grouped_tester_ids = {}
    action_dict = {}


    for idx in overlap_indice:
        if idx in data.index:
            tester_id = data.loc[idx, 'TESTER_ID']
            tran_type = data.loc[idx, 'TRANS_TYPE']

            if str(tran_type).upper() == "SWAP":
                action_dict[idx] = {tester_id:[tester_id,data.loc[idx, 'ERACK_ID_OUT'],data.loc[idx, 'ERACK_ID_IN'],tester_id]}
            elif str(tran_type).upper() == "LOAD":
                action_dict[idx] = {tester_id:[data.loc[idx, 'ERACK_ID_IN'],tester_id]}
            elif str(tran_type).upper() == "UNLOAD":
                action_dict[idx] = {tester_id:[tester_id,data.loc[idx, 'ERACK_ID_OUT']]}


    return action_dict
    
def DistanceSchedualing(overlapIdx,action_dict):


    dataDistance_tmp = data_distance.set_index("TESTER_ID",inplace=False,drop=True)


    after_scheduling_seq = []
    final_sechedualing = False


    distance = 0 
    preID,remaining_indices_for_schedualing = NVGetFirstIndexAndoverlaplist(action_dict,overlapIdx)
   
    # 後面需考慮電子料架的問題

    if Scheduling_type =="fcfs":
        final_sechedualing = remaining_indices_for_schedualing
    if Scheduling_type == "distance": 
        final_sechedualing= sechedualRecursive(distance,remaining_indices_for_schedualing,preID,action_dict,dataDistance_tmp)   
    if Scheduling_type == "random":
        random.shuffle(remaining_indices_for_schedualing) 
        final_sechedualing = remaining_indices_for_schedualing
    final_sechedualing.insert(0,overlapIdx[0])
    return final_sechedualing
  
  
def sechedualRecursive(distance,secheduleList:list,preID,action_dict:dict,dataDistance_tmp):
    
    strat = True 
    min_distance = 0
    min_id = None
    final_list = [] 
    EL_list = ['T1-ES-01', 'T1-ES-02', 'T1-ES-03', 'T1-ES-04', 'T1-ES-05', 'T1-ES-06', 'T1-ES-07', 'T1-ES-08', 'T1-ES-09','AGV1']
    for id in secheduleList: 
        #拿到工作流程的第一個位置

        iddict = action_dict[id][list(action_dict[id].keys())[0]]
        headOfaction = iddict[0]
        tailOfaction = iddict[-1]
        if UseWeight:
            tester_id = list(action_dict[id].keys())[0]
            item_distance = dataDistance_tmp.loc[str(preID),headOfaction]*MachineWeight[MachineWeight["TESTER_ID"] == tester_id].loc[:,"Weight"].values[0]
        else: 
            item_distance = dataDistance_tmp.loc[str(preID),headOfaction]


        if str(preID) in EL_list or str(item_distance)==str(np.nan): 
            if UseWeight:
                item_distance = dataDistance_tmp.loc[headOfaction,str(preID)]*MachineWeight[MachineWeight["TESTER_ID"] == tester_id].loc[:,"Weight"].values[0]
            else: 
                item_distance = dataDistance_tmp.loc[headOfaction,str(preID)]

        if strat:
            min_distance = item_distance
            strat = False
            min_id = id
            
        elif item_distance<min_distance:

            min_distance = item_distance
            min_id = id 
            
    resultDistance = distance+min_distance

    secheduleList.pop(secheduleList.index(min_id))
    if  secheduleList == []:
        final_list.append(min_id)
        return final_list
        # print(min_distance)

    else :
        deeperid = sechedualRecursive(resultDistance,secheduleList,tailOfaction,action_dict,dataDistance_tmp)
        if strat ==False:
            final_list.append(min_id)
        final_list.extend(deeperid)
    return final_list
def DistanceWaitingCalculate(schedualingList:list,action_dict:dict):

    dataDistance_tmp = data_distance.set_index("TESTER_ID",inplace=False,drop=True)
    total_distance = 0 
    EL_list = ['T1-ES-01', 'T1-ES-02', 'T1-ES-03', 'T1-ES-04', 'T1-ES-05', 'T1-ES-06', 'T1-ES-07', 'T1-ES-08', 'T1-ES-09','AGV1']
    final_machine_seq = []
    machine_seq = [] 
    nan_count = 0

    preID,seqList = NVGetFirstIndexAndoverlaplist(action_dict,schedualingList)

    machine_seq.append(preID)
    seq_distance = 0 
    finish_distance = 0 
    for idx in seqList:
        machine_seq.append(list(action_dict[idx].keys())[0])

        last_position = False
        for position in action_dict[idx][list(action_dict[idx].keys())[0]]:

            last_position = position
            if not np.isnan(dataDistance_tmp.loc[position,str(preID)]):
                seq_distance += dataDistance_tmp.loc[position,str(preID)]
                total_distance += seq_distance
                

            else: 

                if  not np.isnan(dataDistance_tmp.loc[str(preID),position]):
                    seq_distance += dataDistance_tmp.loc[str(preID),position]
                    total_distance += seq_distance
                    
                else: 
                    nan_count+=1
            preID = last_position
            
    total_waitting_time = total_distance/0.6   
    finish_time = seq_distance/0.6


    
    return total_waitting_time,machine_seq,finish_time
     
def SecheduallingOuput_csvfil(waitTime,machine_seq,SechedualList):
    
    with open("./scheduling_result.csv","w+") as f : 
        for sech,seq in zip(SechedualList,machine_seq):
            for  persech in sech : 
                f.write(f"{str(persech)},")
            f.write(f"\n") 
            for perSeq in seq : 
                f.write(f"{str(perSeq)},")
            f.write(f"\n")
        f.write("\n")
        f.write(f"Total Waiting time:,{waitTime}(sec)")
        f.write("\n")
        f.write(f"Avarage Waiting time: {waitTime/data_agv.shape[0]} (sec)")
        f.write("\n")
            
def NonDistanceSchedualing(grouped_tester_ids,action_dict):



    # Define the specific columns in the 'Total Distance' sheet we will be working with.
    non_scheduling_seq = []
    for group_number, (remaining_indices, _) in grouped_tester_ids.items():
        distance = 0 
        remaining_indices_for_Nonschedualing = [int(x) for x in remaining_indices.split(',')]
        preID,remaining_indices_for_Nonschedualing = NVGetFirstIndexAndoverlaplist(action_dict,remaining_indices_for_Nonschedualing)
        # gen 
        # 後面需考慮電子料架的問題
        non_scheduling_seq.append(remaining_indices_for_Nonschedualing)

    return non_scheduling_seq

def NVGetFirstIndexAndoverlaplist(action_dict,remaining_indices_for_schedualing):

    first_position = remaining_indices_for_schedualing[0]
    action_dict[first_position][list(action_dict[first_position].keys())[0]]
    tester_id = data_agv.loc[first_position, 'TESTER_ID']
    tran_type = data_agv.loc[first_position, 'TRANS_TYPE']

    if str(tran_type).upper() == "SWAP":
        first_postion_name = tester_id
    elif str(tran_type).upper() == "LOAD":
        first_postion_name = tester_id
    elif str(tran_type).upper() == "UNLOAD":
        first_postion_name = data_agv.loc[first_position, 'ERACK_ID_OUT']
    remaining_indices_for_schedualing = remaining_indices_for_schedualing[1:]

    return first_postion_name,remaining_indices_for_schedualing
@timecounter("main")
def NVSchedulaingMain():
    finalseq,overlapwaitingtime = GetOverlapIndex(data_agv)
    
    nonOverlapIndex = GetNonOverlapIndex(finalseq)
    
    nonOverlapWaitingTime = GetNonoverlapIndexWattingTime(nonOverlapIndex)
    totalwaitintime = overlapwaitingtime+nonOverlapWaitingTime
    avarage_waiting_time = totalwaitintime/ data.shape[0]
    
    print(f"Avarage waitting time {avarage_waiting_time}")
    print(f"Final waitting time {totalwaitintime}")

 
    
if __name__ == "__main__":
    NVSchedulaingMain()


