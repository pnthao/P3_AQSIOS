#ifndef _STREAM_SOURCE_
#include "execution/operators/stream_source.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _OUTPUT_
#include "execution/operators/output.h"
#endif

#include <string.h>
#include <math.h>
// Response Time Calculation By Lory Al Moakar
#ifndef _TIMER_
#include "execution/monitors/timer.h"
#endif
//end of part 1 of response time calculation by LAM

#include <stdlib.h>
#include<stdio.h>

using namespace Execution;
using namespace std;

StreamSource::StreamSource (unsigned int _id, std::ostream& _LOG)
	: LOG (_LOG)
{
	id             = _id;
	outputQueue    = 0;
	storeAlloc     = 0;
	numAttrs       = 0;
	source         = 0;
	lastInputTs    = 0;
	lastOutputTs   = 0;

	//HR implementation by Lory Al Moakar
	//initialize operator type and statistics 
	
	this -> operator_type = STREAM_SOURCE;
	this -> mean_cost = 1;
	this -> mean_selectivity = 1;
	this -> local_selectivity =1;
	this -> n_tuples_outputted = 0;
	this -> local_cost = 1;
	// we want input operators to have a high priority 
	// because we want to input tuples at a fast pace
	// as close as possible to when they arrive
	this -> priority = 10; 
	this -> num_tuples_processed = 0;
	
	//end of part 1 of HR implementation by LAM
	
	//Load managing, by Thao Pham

	//the timestamp of the tuple starting the cycle of input rate computation
	startTs 	   = 0;
	//number of input tuples so far in the 	cycle of input rate computation
	numOfIncomingTuples = 0;
	this-> isShedder = false;
	this->input_load =0; //when isShedder is true, this operator is not a source load and therefore its input load is the input rate at the corresponding source * (product of all preceding ops)
	this->drop_percent =0;
	this->snapshot_local_cost_per_tuple = 0;
	this->loadCoefficient = 0;
	this->snapshot_loadCoefficient = 0;
	
	dropCount    = 0;
	tupleCount   = 0;
		
#ifdef _CTRL_LOAD_MANAGE_	
	ctrl_out_tuple_count = 0;
	//ctrl_input_rates = fopen("/home/thao/workspace/stream.test.thao/load_managing/tracking_pareto_40000_70000_5ms_combined","r");
	//ctrl_input_rates = fopen("/home/thao/workspace/stream.test.thao/load_managing/tracking_pareto_60000_90000_5ms","r");
	//ctrl_input_rates = fopen("/home/thao/workspace/stream.test.thao/load_managing/tracking_const_40000_10s","r");
	//ctrl_input_rates = fopen("/home/thao/workspace/stream.test.thao/load_managing/tracking_realtcp", "r");
	//ctrl_input_rates = fopen("/home/thao/workspace/stream.test.thao/load_managing/tracking_const_350", "r");
	
	ctrl_current_rate = 0;
	ctrl_rate_effective_time = 0;
	ctrl_num_cycle = 0;
	ctrl_last_ts = 0;
	ctrl_compensation = 0;
	ctrl_num_of_queuing_tuples = 0;	
	ctrl_actual_total_incoming_tuples = 0;
	ctrl_estimated_total_incoming_tuples = 0;
	ctrl_checkpoint_estimated_total_incoming_tuples = 0;
	ctrl_checkpoint_ts = 0;
	ctrl_adjusted = true;
#endif
	//end of load managing, by Thao Pham

	//ArmaDILoS, by Thao Pham

	startTupleTs = 0;
	stopTupleTs = 0;
	pthread_mutex_init(&mutex_startTupleTs, NULL);
	pthread_mutex_init(&mutex_file_handle,NULL);
}

StreamSource::~StreamSource() {
#ifdef _CTRL_LOAD_MANAGE_
if(ctrl_input_rates)
	fclose(ctrl_input_rates);
#endif	
pthread_mutex_destroy(&mutex_startTupleTs);
pthread_mutex_destroy(&mutex_file_handle);
}

int StreamSource::setOutputQueue (Queue *outputQueue)
{
	ASSERT (outputQueue);

	this -> outputQueue = outputQueue;
	return 0;
}

int StreamSource::setStoreAlloc (StorageAlloc *storeAlloc)
{
	ASSERT (storeAlloc);

	this -> storeAlloc = storeAlloc;
	return 0;
}

int StreamSource::setTableSource (Interface::TableSource *source)
{
	ASSERT (source);

	this -> source = source;
	return 0;
}

int StreamSource::addAttr (Type type, unsigned int len, Column outCol)
{
	// We do not have space
	if (numAttrs == MAX_ATTRS)
		return -1;

	attrs [numAttrs].type = type;
	attrs [numAttrs].len = len;
	outCols [numAttrs] = outCol;
	
	numAttrs ++;
	return 0;
}

int StreamSource::initialize ()
{
	int rc;
	unsigned int offset;
	
	ASSERT (numAttrs > 0);
	ASSERT (source);
	
	offset = DATA_OFFSET;
	for (unsigned int a = 0 ; a < numAttrs ; a++) {
		offsets [a] = offset;
		
		switch (attrs [a].type) {
		case INT:   offset += INT_SIZE; break;
		case FLOAT: offset += FLOAT_SIZE; break;
		case BYTE:  offset += BYTE_SIZE; break;
		case CHAR:  offset += attrs[a].len; break;
		default:
			return -1;
		}
	}
	
	if ((rc = source -> start ()) != 0)
		return -23;
	
	return 0;
}

int StreamSource::run (TimeSlice timeSlice)
{
		
	if(status==START_PENDING)
		return run_in_start_pending(timeSlice);
	if(status ==START_PREPARING)
		return run_in_start_preparing(timeSlice);
	if(status == STOP_PREPARING)
		return run_in_stop_preparing(timeSlice);

	int rc;
	unsigned int  numElements;
	char         *inputTuple;
	Timestamp     inputTs;
	unsigned int  inputTupleLen;
	Tuple         outputTuple;
	bool          bHeartbeat;
	
#ifdef _MONITOR_
	startTimer ();
#endif							
	
	numElements = timeSlice;	
	
	int max_delay = 0;
	
	//local stats computation, by Thao Pham
	Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();
	int n_before = num_tuples_processed;
	//end of part 1 of local stats computation, by Thao Pham
	unsigned int e =0;

	//to save time, this mutex is locked at the beginning of this function instead of everytime a tuple is read
	pthread_mutex_lock(&mutex_file_handle);

	for (e = 0 ; e < numElements ; e++) {

		// We are blocked @ the output queue
			if (outputQueue -> isFull()) 
				break;
			
		// Response Time Calculation By Lory Al Moakar
		//get the current time
		Monitor::Timer mytime;
		unsigned long long int curTime = mytime.getTime(); 
	
		//calculate time elapsed
		curTime = curTime - this -> system_start_time;
	
		//added by Thao Pham : curTime in CPU-time measurement (after switching getTime to read realtime):
		unsigned long long int curCPUTime = mytime.getCPUTime();
		
		curCPUTime = curCPUTime - system_start_CPUtime;
		
		//end of cur real time added by Thao Pham

		//calculate time elapsed in time units
		unsigned long long int time_elapsed = curTime / time_unit;
	
		// Get the next input tuple
		
		
		if ((rc = source -> getNext (inputTuple,
					     inputTupleLen,
					     bHeartbeat,
					     time_elapsed)) != 0)
			return rc;
		
		//end of part 2 of response time calculation by LAM
		// We do not have an input tuple yet
		if (!inputTuple)
			break;
			
		// Get the timestamp: which is the first field
		memcpy (&inputTs, inputTuple, TIMESTAMP_SIZE);
	
		// We should have a progress of time.
		if (lastInputTs > inputTs) {
			LOG << "StreamSource: input not in timestamp order" << endl;
			return -1;
		}
		
		//input rate computation, by Thao Pham
		numOfIncomingTuples ++;
		//end of input rate computation, by Thao Pham
		lastInputTs = inputTs;
		
		//DROP OR KEEP

		bool drop = 0;
		
		unsigned int r = rand()%100+1; //random number from 0-100
		if(r <= this->drop_percent){
			drop = 1;
		}

		//num_tuples_processed += 1;//if we put it here, we don't count the cost of those dropped
		
		if(!drop)
		{
			//local cost computation, by Thao Pham (selectivity of input is always equals to the drop_percent)
			// find out the num_tuples_processed so far
			
			num_tuples_processed += 1;
			
			// end of part 2 of local cost computation, by Thao Pham

			//HR implementation by Lory Al Moakar
			int x = time_elapsed - lastInputTs ;
			if ( x > max_delay ) 
			  max_delay = x;
			// end of part 1 of HR implementation by LAM

			// Ignore heartbeats
			if (bHeartbeat) {
				LOG << "Heartbeat received" << endl;
				continue;
			}
			
#ifdef _CTRL_LOAD_MANAGE_
			for(int i=0;i<numOutputs;i++)
			{
				this->outputs[i]->ctrl_num_of_queuing_tuples +=1;
			}
			this->ctrl_num_of_queuing_tuples -=1;
			ctrl_actual_total_incoming_tuples +=1;

#endif //_CTRL_LOAD_MANAGE	
			
		
			// Get the storage for the output tuple
			if ((rc = storeAlloc -> newTuple (outputTuple)) != 0)
				return rc;
		
			// Get the attributes
			for (unsigned int a = 0 ; a < numAttrs ; a++) {
				switch (attrs [a].type) {				
				case INT:
					memcpy (&ICOL(outputTuple, outCols[a]), 
							inputTuple + offsets[a], INT_SIZE);
					break;
					
				case FLOAT:
					memcpy (&FCOL(outputTuple, outCols[a]),
							inputTuple + offsets[a], FLOAT_SIZE);
					break;
					
				case BYTE:
					BCOL(outputTuple, outCols[a]) = inputTuple[offsets[a]];
					break;
					
				case CHAR:
					strncpy (CCOL(outputTuple, outCols[a]),
							 inputTuple + offsets[a],
							 attrs[a].len);
					break;
					
				default:
					// Should not come
					return -1;
				}
			}
		
			outputQueue -> enqueue (Element(E_PLUS, outputTuple, inputTs));
			lastOutputTs = inputTs;
	
		}

		this->tupleCount = this->tupleCount+1;
		
	}
	
	// Heartbeat generation: Assert to the operator above that we won't
	// produce any element with timestamp < lastInputTs
	
	if (!outputQueue -> isFull() && (lastInputTs > lastOutputTs)) {
		outputQueue -> enqueue (Element::Heartbeat(lastInputTs));
		lastOutputTs = lastInputTs;
	}
	
	pthread_mutex_unlock(&mutex_file_handle);

#ifdef _MONITOR_
	stopTimer ();
	logOutTs (lastOutputTs);		
#endif							
	
	//local cost computation, by Thao Pham
	//get current time	
	unsigned long long int timeAfterLoop = mytime.getCPUTime();
	
	//calculate time elapsed during execution
	//add it to local cost
	if ( num_tuples_processed > n_before )//e >= 1  
	  local_cost += timeAfterLoop - timeBeforeLoop;
	//printf("%lld\n", local_cost);
	//end of part 3 of cost computation by Thao Pham
	
	
	return 0;
}

		
//get the load of the input, i.e, <input rate>*<load coefficient>
int StreamSource::getLoad(double& load, double& effective_load, double &source_load)
{
	load = this->loadCoefficient * this->inputRate* (100.0-(double)this->drop_percent)/100.0;
	effective_load = effective_loadCoefficient*inputRate*(100.0-(double)this->drop_percent)/100.0;
	source_load = this->effective_cost*inputRate*(100.0-(double)this->drop_percent)/100.0;

	//printf ("\n source get load \n");
	return 0;
}

/*double StreamSource::getCurLoad()
{
	return (snapshot_loadCoefficient*inputRate);
}
double StreamSource::getExpectedHeavyLoad()
{
	return (effective_loadCoefficient*inputRate);
}
double StreamSource::getSourceLoad()
{
	return (this->snapshot_local_cost_per_tuple*inputRate);
}		*/
//reset the input rate computation cycle 
int StreamSource::resetInputRateComputationCycle()
{
	this->startTs = this->lastInputTs;
	this->numOfIncomingTuples = 0;
	return 0;
}

//set the input rate to new value		
bool StreamSource::computeInputRate()
{
	
	if (lastInputTs ==1||(this->lastInputTs - this->startTs == 0)) return false; //nothing processed yet
	
	//inter-arrival time in nanosec, note that timetamps of input tuples are in time unit
	//double interArrivalTime = ((this->lastInputTs - this->startTs)*time_unit)/this->numOfIncomingTuples;
	
	this->inputRate =  (double)(input_rate_time_unit*numOfIncomingTuples)/(double)((this->lastInputTs - this->startTs)*time_unit);  //input_rate_time_unit/interArrivalTime;
	//printf("%f\n", this->inputRate);
	return true;
}

#ifdef _CTRL_LOAD_MANAGE_
void StreamSource::push_drop_info(unsigned long long int drop, unsigned long long int time)
{
	//Drop_info info;
	//info.drop = drop;
	//info.effective_time = time;
	//printf("%lld\n",drop);
	
	drop_info_queue.push(time-this->system_start_time); 	
	drop_info_queue.push(drop);
	//printf("hum %lld\n",drop_info_queue.front());
}
#endif
//end of load managing, by Thao Pham
								
//HR implementation by Lory Al Moakar
/**
 * This method is used in order to calculate the local 
 * selectivity and the local cost per tuple of this operator
 * @return bool true or false
 * true when the statistics have changed and false otherwise
 */ 
bool StreamSource::calculateLocalStats(){ 

//implementation added by Thao Pham

// no tuples were processed in the last round
  // --> no need to adjust costs and selectivity
  if ( num_tuples_processed == 0) {
    return false;
  }
 
  //selectivity = 1 - drop_percent	  
  //local_selectivity = 1; //this is for the control-based scheme
  local_selectivity = 1;//1-((double)drop_percent/(double)100); //this is for the HB scheme

 //calculate local cost
  if (!firstRefresh) { 
  	
    double old_local_cost = local_cost_per_tuple;
    double current_avg = (double) local_cost / (double) num_tuples_processed;

    local_cost_per_tuple = (1-ALPHA) * local_cost_per_tuple +
      ALPHA * current_avg; // /time_unit;
     
    k_measurements += 1;
    
    M_k = M_k_1 + ( current_avg - M_k_1)/k_measurements;
    S_k += ( current_avg - M_k_1 ) * ( current_avg - M_k );
    M_k_1 = M_k;
    
    avg_k = (1-ALPHA) * avg_k +  ALPHA * current_avg; 
    
    stdev = sqrt ( S_k / ( k_measurements -1 ) ) ;
    
    if ( (current_avg - avg_k) > OULIER_FACTOR*stdev) {
      //      printf("SELECT OUTLIER current %f  new avg %f old avg %f \n", current_avg, local_cost_per_tuple,
      //     old_local_cost);
      local_cost_per_tuple  = old_local_cost;
      
    }
    
    //printf ( "SELECTION OPERATOR %d costs %llu  n_tuples %d smoothed %f \n",
    //     operator_id, local_cost, num_tuples_processed,local_cost_per_tuple);
  }
  else {
    
    local_cost_per_tuple = (double) local_cost / (double) num_tuples_processed; // /time_unit;
    M_k = local_cost_per_tuple;
    avg_k = local_cost_per_tuple;
    M_k_1 = M_k;
    S_k = 0;
    k_measurements = 1;
    //printf ( "OPERATOR %d costs %llu  n_tuples %d\n",
    //	     operator_id, local_cost, num_tuples_processed);
  }
  
  firstRefresh = false;
//end of the implementation added by Thao Pham

//load manager, by Thao Pham
  snapshot_local_cost_per_tuple = (double) local_cost / (double) num_tuples_processed;
  //end of load manager, by Thao Pham 	
return true; 
}

/** this method is used in order to calculate the priority
 * of the operator. This is used in order to assign the priorities
 * used by the scheduler to schedule operators.
 */

void StreamSource::refreshPriority(){
}
// end of part 1 of HR implementation by LAM

//HR with ready by Lory Al Moakar 
/** this method returns how ready this operator is to 
 * execute. If it returns a positive number --> ready
 * If it returns a zero or negative number --> not ready
 */
int StreamSource::readyToExecute() {
  //op not ready when it has a full output queue --> no space to output tuples
  // or op has read all of its input
  if ( outputQueue->isFull() || source ->end() ) 
    return 0;
  return 1;
}

// end of part 1 of HR with ready by LAM

//added by Thao Pham, to simulate the case when the system reads input from a memory buffer instead of a file
int StreamSource::loadAllInput()
{
	 return this->source->loadSourceData();

}
#ifdef _CTRL_LOAD_MANAGE_
double StreamSource::get_incoming_tuples(unsigned long long int &cur_ts)
{
	Monitor::Timer timer;
	//unsigned long long int cur_time = timer.getTime() - system_start_time;
	unsigned long long int cur_time = cur_ts - system_start_time;
	if(ctrl_last_ts ==0)
		ctrl_last_ts = 0;
		
	unsigned long long int ctrl_period = cur_time - ctrl_last_ts;
	ctrl_last_ts = cur_time;
	
	//printf("%lld \n", ctrl_period);
	
	unsigned long long int rate_length = 10000000; //a rate is stable for 10ms  - sp3
	if(!feof(ctrl_input_rates) && ctrl_rate_effective_time == 0){
		ctrl_rate_effective_time += rate_length*1000;
		int rate;
		fscanf(ctrl_input_rates, "%d",&rate);
		ctrl_current_rate = rate;
		//printf("%d\n", ctrl_current_rate);
		ctrl_sum = 0;
	}
	//printf("rate: %f\n", ctrl_current_rate);
	double  num_of_incoming_tuples=0;
	if(cur_time < ctrl_rate_effective_time){
		//num_of_incoming_tuples = ctrl_current_rate*((double)ctrl_period/(double)input_rate_time_unit);
		num_of_incoming_tuples = ctrl_current_rate*((double)ctrl_period/(double)(rate_length*1000));
		//printf("tuples: %f, curtime: %lld, eff time: %lld\n", num_of_incoming_tuples, cur_time, ctrl_rate_effective_time);
		ctrl_sum += num_of_incoming_tuples;

		//printf("%f\n", num_of_incoming_tuples);
		return num_of_incoming_tuples;

	}
	else
	{
		//the current rate is to be applied to the period of (ctrl_period-(cur_time - effectivetime))
		long long int first_period = ctrl_period - (cur_time - ctrl_rate_effective_time);
		if(first_period < 0) first_period = 0;

		//num_of_incoming_tuples = ctrl_current_rate*((double)first_period/(double)input_rate_time_unit);
		num_of_incoming_tuples = ctrl_current_rate*((double)first_period/(double)(rate_length*1000));
		ctrl_sum += num_of_incoming_tuples;

		while((cur_time > ctrl_rate_effective_time) && ctrl_rate_effective_time > 0)
		{
			num_of_incoming_tuples += ctrl_current_rate - ctrl_sum;
			//printf("%f\n", ctrl_current_rate - ctrl_sum);
			if(!feof(ctrl_input_rates))
			{
				ctrl_sum = 0;
				int rate;
				if(fscanf(ctrl_input_rates, "%d",&rate)!=EOF){
					ctrl_current_rate = (double)rate;

					long long int next_period;
					if((cur_time - ctrl_rate_effective_time) < rate_length*1000)
						next_period = cur_time - ctrl_rate_effective_time;
					else
						next_period = rate_length*1000;

					//printf("hah");
					//num_of_incoming_tuples += ctrl_current_rate*(x/(double)input_rate_time_unit);
					double t = ctrl_current_rate*(((double)next_period)/(double)(rate_length*1000));

					num_of_incoming_tuples += t;
					ctrl_sum += t;

					//printf("%f\n", num_of_incoming_tuples);
					ctrl_rate_effective_time +=rate_length*1000;
					//if(cur_time>ctrl_rate_effective_time) printf("haha\n");
				}
				else
				{
					ctrl_current_rate = 0;
					ctrl_rate_effective_time =0;
				}
			}
			else
			{
				ctrl_current_rate = 0;
				ctrl_sum = 0;
				ctrl_rate_effective_time =0;
			}
		}

		return num_of_incoming_tuples;
	}
	
}

#endif //_CTRL_LOAD_MANAGE_

int StreamSource::run_with_shedder (TimeSlice timeSlice)//is_shedder is always false for a true source (although a true source is always has embedded shedder)
{
	return 0;
}

//ArmaDILoS

std::streampos StreamSource::getCurPos(){
	return source->getCurPos();
}
Timestamp StreamSource::startDataReading(std::streampos curPos){
	Timestamp ts = source->startDataReading(curPos);
	this->status = START_PENDING;
	return ts;
}

int StreamSource::run_in_start_pending(TimeSlice timeSlice)
{

	int rc;
	unsigned int  numElements;
	char         *inputTuple;
	Timestamp     inputTs;
	unsigned int  inputTupleLen;
	Tuple         outputTuple;
	bool          bHeartbeat;

#ifdef _MONITOR_
	startTimer ();
#endif

	numElements = timeSlice;

	int max_delay = 0;

	//local stats computation - not needed for this status
	/*Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();
	int n_before = num_tuples_processed;*/
	//end of part 1 of local stats computation, by Thao Pham
	unsigned int e =0;
	for (e = 0 ; e < numElements ; e++) {
		//check if the startTupleTS has been set
		pthread_mutex_lock(&mutex_startTupleTs);
		if(startTupleTs>0){
			//switch to "start_preparing" mode:
			#ifdef _MONITOR_
				stopTimer ();
			#endif
			return run_in_start_preparing(timeSlice-e);
		}

		pthread_mutex_unlock(&mutex_startTupleTs);

		// Response Time Calculation By Lory Al Moakar
		//get the current time
		Monitor::Timer mytime;
		unsigned long long int curTime = mytime.getTime();

		//calculate time elapsed
		curTime = curTime - this -> system_start_time;

		//added by Thao Pham : curTime in CPU-time measurement (after switching getTime to read realtime):
		unsigned long long int curCPUTime = mytime.getCPUTime();

		curCPUTime = curCPUTime - system_start_CPUtime;

		//end of cur real time added by Thao Pham

		//calculate time elapsed in time units
		unsigned long long int time_elapsed = curTime / time_unit;

		// Get the next input tuple

		if ((rc = source -> getNext (inputTuple,
					     inputTupleLen,
					     bHeartbeat,
					     time_elapsed)) != 0)
			return rc;

		// We do not have an input tuple yet
		if (!inputTuple)
			break;

		// Ignore heartbeats
		if (bHeartbeat) {
			LOG << "Heartbeat received" << endl;
			continue;
		}

		//add input tuple into a temporary queue
		char* t = new char[inputTupleLen];
		memcpy(t,inputTuple,inputTupleLen);
		pending_tuples.push(t);

		/////////////////////////

		// We should have a progress of time.
		if (lastInputTs > inputTs) {
			LOG << "StreamSource: input not in timestamp order" << endl;
			return -1;
		}

	}
#ifdef _MONITOR_
	stopTimer ();
#endif
	return 0;
}

/////////////////////////
int StreamSource::run_in_start_preparing(TimeSlice timeSlice)
{


	int rc;
	unsigned int  numElements;
	char         *inputTuple;
	Timestamp     inputTs;
	unsigned int  inputTupleLen;
	Tuple         outputTuple;
	bool          bHeartbeat;

#ifdef _MONITOR_
	startTimer ();
#endif

	numElements = timeSlice;

	int max_delay = 0;

	//local stats computation, by Thao Pham
	Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();
	int n_before = num_tuples_processed;
	//end of part 1 of local stats computation, by Thao Pham
	unsigned int e =0;
	for (e = 0 ; e < numElements ; e++) {

		// We are blocked @ the output queue
			if (outputQueue -> isFull())
				break;

		// Response Time Calculation By Lory Al Moakar
		//get the current time
		Monitor::Timer mytime;
		unsigned long long int curTime = mytime.getTime();

		//calculate time elapsed
		curTime = curTime - this -> system_start_time;

		//added by Thao Pham : curTime in CPU-time measurement (after switching getTime to read realtime):
		unsigned long long int curCPUTime = mytime.getCPUTime();

		curCPUTime = curCPUTime - system_start_CPUtime;

		//end of cur real time added by Thao Pham

		//calculate time elapsed in time units
		unsigned long long int time_elapsed = curTime / time_unit;

		bool is_from_pending_queue = false;
		// Get the next input tuple, from the temporary queue instead of the source file
		if(status==START_PREPARING && pending_tuples.size()>0){
			// Get the next input tuple, from the temporary queue instead of the source file
			inputTuple = pending_tuples.front();
			pending_tuples.pop();
			is_from_pending_queue = true;
			bHeartbeat = false;
			if(pending_tuples.size()==0)
				status = ACTIVE;
		}

		else{

			if ((rc = source -> getNext (inputTuple,
							 inputTupleLen,
							 bHeartbeat,
							 time_elapsed)) != 0)
				return rc;

		}

		// We do not have an input tuple yet
		if (!inputTuple)
			break;

		// Get the timestamp: which is the first field
		memcpy (&inputTs, inputTuple, TIMESTAMP_SIZE);
		if(inputTs < startTupleTs){
			if(is_from_pending_queue){
				delete[] inputTuple;
				inputTuple = 0;
			}
			continue;//skip the tuples
		}
		// We should have a progress of time.
		if (lastInputTs > inputTs) {
			LOG << "StreamSource: input not in timestamp order" << endl;
			return -1;
		}

		//input rate computation, by Thao Pham
		numOfIncomingTuples ++;
		//end of input rate computation, by Thao Pham
		lastInputTs = inputTs;

		//DROP OR KEEP

		bool drop = 0;

		unsigned int r = rand()%100+1; //random number from 0-100
		if(r <= this->drop_percent){
			drop = 1;
		}

		//num_tuples_processed += 1;//if we put it here, we don't count the cost of those dropped

		if(!drop)
		{
			//local cost computation, by Thao Pham (selectivity of input is always equals to the drop_percent)
			// find out the num_tuples_processed so far

			num_tuples_processed += 1;

			// end of part 2 of local cost computation, by Thao Pham

			//HR implementation by Lory Al Moakar
			int x = time_elapsed - lastInputTs ;
			if ( x > max_delay )
			  max_delay = x;
			// end of part 1 of HR implementation by LAM

			// Ignore heartbeats
			if (bHeartbeat) {
				LOG << "Heartbeat received" << endl;
				continue;
			}

#ifdef _CTRL_LOAD_MANAGE_
			for(int i=0;i<numOutputs;i++)
			{
				this->outputs[i]->ctrl_num_of_queuing_tuples +=1;
			}
			this->ctrl_num_of_queuing_tuples -=1;
			ctrl_actual_total_incoming_tuples +=1;

#endif //_CTRL_LOAD_MANAGE


			// Get the storage for the output tuple
			if ((rc = storeAlloc -> newTuple (outputTuple)) != 0)
				return rc;

			// Get the attributes
			for (unsigned int a = 0 ; a < numAttrs ; a++) {
				switch (attrs [a].type) {
				case INT:
					memcpy (&ICOL(outputTuple, outCols[a]),
							inputTuple + offsets[a], INT_SIZE);
					break;

				case FLOAT:
					memcpy (&FCOL(outputTuple, outCols[a]),
							inputTuple + offsets[a], FLOAT_SIZE);
					break;

				case BYTE:
					BCOL(outputTuple, outCols[a]) = inputTuple[offsets[a]];
					break;

				case CHAR:
					strncpy (CCOL(outputTuple, outCols[a]),
							 inputTuple + offsets[a],
							 attrs[a].len);
					break;

				default:
					// Should not come
					return -1;
				}
			}

			outputQueue -> enqueue (Element(E_PLUS, outputTuple, inputTs));
			lastOutputTs = inputTs;

		}

		this->tupleCount = this->tupleCount+1;
		if(is_from_pending_queue){
			delete[] inputTuple;
			inputTuple = 0;
		}

	}

	// Heartbeat generation: Assert to the operator above that we won't
	// produce any element with timestamp < lastInputTs

	if (!outputQueue -> isFull() && (lastInputTs > lastOutputTs)) {
		outputQueue -> enqueue (Element::Heartbeat(lastInputTs));
		lastOutputTs = lastInputTs;
	}

#ifdef _MONITOR_
	stopTimer ();
	logOutTs (lastOutputTs);
#endif

	//local cost computation, by Thao Pham
	//get current time
	unsigned long long int timeAfterLoop = mytime.getCPUTime();

	//calculate time elapsed during execution
	//add it to local cost
	if ( num_tuples_processed > n_before )//e >= 1
	  local_cost += timeAfterLoop - timeBeforeLoop;
	//printf("%lld\n", local_cost);
	//end of part 3 of cost computation by Thao Pham


	return 0;
}

/////////////////////////
void StreamSource::setStartTupleTS(Timestamp start_ts){
	pthread_mutex_lock(&mutex_startTupleTs);
	startTupleTs = start_ts;
	pthread_mutex_unlock(&mutex_startTupleTs);
	//switch the source to start_preparing mode;
	status = START_PREPARING;
}

Timestamp StreamSource::getStartTupleTS(Timestamp dest_startTs){
	pthread_mutex_lock(&mutex_file_handle);
	if(dest_startTs>lastInputTs)
		stopTupleTs = dest_startTs;
	else
		stopTupleTs = lastInputTs;
	prepareToStop(this,stopTupleTs);
	pthread_mutex_unlock(&mutex_file_handle);
	return stopTupleTs;
}
void StreamSource::prepareToStop(Operator *op, Timestamp stopTS){
	if(op->operator_type==STREAM_SOURCE||op->operator_type==PARTN_WIN
			||op->operator_type==RANGE_WIN||op->operator_type==ROW_WIN)
		if(!isWindowDownstream(op)){
			op->status = START_PREPARING;
			op->stopTupleTs = stopTS;
		}
	for(int i=0;i<op->numOutputs;i++){
		if(op->outputs[i]->operator_type!=OUTPUT)
			prepareToStop(op->outputs[i], stopTS);
	}
}
bool StreamSource::isWindowDownstream(Operator *op){
	//TODO: this implementation now only works with plan without sharing.
	//for plan with sharing, the Operator structure needs to be augmented to include a list of query ID
	//the Operator belongs to
	for(int i=0;i<op->numOutputs; i++){
		if (op->outputs[i]->operator_type == PARTN_WIN || op->outputs[i]->operator_type ==RANGE_WIN
				|| op->outputs[i]->operator_type == ROW_WIN)
				return true;
	}

	for(int i=0;i<op->numOutputs;i++)
		if (isWindowDownstream(op->outputs[i])) return true;

	return false;
}

int StreamSource::run_in_stop_preparing(TimeSlice timeSlice){
	int rc;
	unsigned int  numElements;
	char         *inputTuple;
	Timestamp     inputTs;
	unsigned int  inputTupleLen;
	Tuple         outputTuple;
	bool          bHeartbeat;

#ifdef _MONITOR_
	startTimer ();
#endif

	numElements = timeSlice;

	int max_delay = 0;

	//local stats computation, by Thao Pham
	Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();
	int n_before = num_tuples_processed;
	//end of part 1 of local stats computation, by Thao Pham
	unsigned int e =0;

	//to save time, this mutex is locked at the beginning of this function instead of everytime a tuple is read
	pthread_mutex_lock(&mutex_file_handle);

	for (e = 0 ; e < numElements ; e++) {

		// We are blocked @ the output queue
			if (outputQueue -> isFull())
				break;

		// Response Time Calculation By Lory Al Moakar
		//get the current time
		Monitor::Timer mytime;
		unsigned long long int curTime = mytime.getTime();

		//calculate time elapsed
		curTime = curTime - this -> system_start_time;

		//added by Thao Pham : curTime in CPU-time measurement (after switching getTime to read realtime):
		unsigned long long int curCPUTime = mytime.getCPUTime();

		curCPUTime = curCPUTime - system_start_CPUtime;

		//end of cur real time added by Thao Pham

		//calculate time elapsed in time units
		unsigned long long int time_elapsed = curTime / time_unit;

		// Get the next input tuple


		if ((rc = source -> getNext (inputTuple,
					     inputTupleLen,
					     bHeartbeat,
					     time_elapsed)) != 0)
			return rc;

		//end of part 2 of response time calculation by LAM
		// We do not have an input tuple yet
		if (!inputTuple)
			break;

		// Get the timestamp: which is the first field
		memcpy (&inputTs, inputTuple, TIMESTAMP_SIZE);

		//if it's time to stop this operator
		if(inputTs>=stopTupleTs){
			deactivate();
			break;
		}


		// We should have a progress of time.
		if (lastInputTs > inputTs) {
			LOG << "StreamSource: input not in timestamp order" << endl;
			return -1;
		}

		//input rate computation, by Thao Pham
		numOfIncomingTuples ++;
		//end of input rate computation, by Thao Pham
		lastInputTs = inputTs;

		//DROP OR KEEP

		bool drop = 0;

		unsigned int r = rand()%100+1; //random number from 0-100
		if(r <= this->drop_percent){
			drop = 1;
		}

		//num_tuples_processed += 1;//if we put it here, we don't count the cost of those dropped

		if(!drop)
		{
			//local cost computation, by Thao Pham (selectivity of input is always equals to the drop_percent)
			// find out the num_tuples_processed so far

			num_tuples_processed += 1;

			// end of part 2 of local cost computation, by Thao Pham

			//HR implementation by Lory Al Moakar
			int x = time_elapsed - lastInputTs ;
			if ( x > max_delay )
			  max_delay = x;
			// end of part 1 of HR implementation by LAM

			// Ignore heartbeats
			if (bHeartbeat) {
				LOG << "Heartbeat received" << endl;
				continue;
			}

#ifdef _CTRL_LOAD_MANAGE_
			for(int i=0;i<numOutputs;i++)
			{
				this->outputs[i]->ctrl_num_of_queuing_tuples +=1;
			}
			this->ctrl_num_of_queuing_tuples -=1;
			ctrl_actual_total_incoming_tuples +=1;

#endif //_CTRL_LOAD_MANAGE


			// Get the storage for the output tuple
			if ((rc = storeAlloc -> newTuple (outputTuple)) != 0)
				return rc;

			// Get the attributes
			for (unsigned int a = 0 ; a < numAttrs ; a++) {
				switch (attrs [a].type) {
				case INT:
					memcpy (&ICOL(outputTuple, outCols[a]),
							inputTuple + offsets[a], INT_SIZE);
					break;

				case FLOAT:
					memcpy (&FCOL(outputTuple, outCols[a]),
							inputTuple + offsets[a], FLOAT_SIZE);
					break;

				case BYTE:
					BCOL(outputTuple, outCols[a]) = inputTuple[offsets[a]];
					break;

				case CHAR:
					strncpy (CCOL(outputTuple, outCols[a]),
							 inputTuple + offsets[a],
							 attrs[a].len);
					break;

				default:
					// Should not come
					return -1;
				}
			}

			outputQueue -> enqueue (Element(E_PLUS, outputTuple, inputTs));
			lastOutputTs = inputTs;

		}

		this->tupleCount = this->tupleCount+1;

	}

	// Heartbeat generation: Assert to the operator above that we won't
	// produce any element with timestamp < lastInputTs

	if (!outputQueue -> isFull() && (lastInputTs > lastOutputTs)) {
		outputQueue -> enqueue (Element::Heartbeat(lastInputTs));
		lastOutputTs = lastInputTs;
	}

	pthread_mutex_unlock(&mutex_file_handle);

#ifdef _MONITOR_
	stopTimer ();
	logOutTs (lastOutputTs);
#endif

	//local cost computation, by Thao Pham
	//get current time
	unsigned long long int timeAfterLoop = mytime.getCPUTime();

	//calculate time elapsed during execution
	//add it to local cost
	if ( num_tuples_processed > n_before )//e >= 1
	  local_cost += timeAfterLoop - timeBeforeLoop;
	//printf("%lld\n", local_cost);
	//end of part 3 of cost computation by Thao Pham


	return 0;
}

void StreamSource::deactivate(){
	status = INACTIVE;
}
