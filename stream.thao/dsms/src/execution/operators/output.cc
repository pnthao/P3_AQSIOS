#ifndef _OUTPUT_
#include "execution/operators/output.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

//added by Thao
#include <string.h>
#include <stdlib.h>
#include <memory.h>
#include <math.h>
#include <stdio.h>
//end of added by Thao

/// debug
#include <iostream>
using namespace std;

using namespace Execution;

#define UNLOCK_INPUT_TUPLE(t) (inStore -> decrRef ((t)))

Output::Output(unsigned int _id, std::ostream& _LOG)
: LOG (_LOG)
{
	id              = _id;
	inputQueue      = 0;
	inStore         = 0;
	numAttrs        = 0;
	output          = 0;
#ifdef _DM_
	lastInputTs     = 0;

	//HR implementation by Lory Al Moakar
	//initialize operator type and statistics 

	this -> operator_type = OUTPUT;
	this -> mean_cost = 1;
	this -> mean_selectivity = 1;
	this -> local_selectivity =1;
	this -> n_tuples_outputted = 0;
	this -> local_cost = 1;
	// we want output operators to have a high priority 
	// because we want to drain tuples fast from the 
	// system
	this -> priority = 10; 
	this -> num_tuples_processed = 0;

	this -> steady_state = false;
	this -> n_full_queue = 0;
	//end of part 1 of HR implementation by LAM

	//load manager , by Thao Pham
	sum_response_time = 0;
	avg_response_time = 0;
	rt_first_ts = 0;
	rt_last_ts = 0;
	pre_avg_responsetime = 0;
	pre_rt_timestamp = 0;

	std_response_time =0; //assumed to be equal to processing time

	num_tuples_rt =0;

	this->snapshot_local_cost_per_tuple = 0;
	this-> isShedder = false;
	this->input_load =0; //when isShedder is true, this operator is not a source load and therefore its input load is the input rate at the corresponding source * (product of all preceding ops)
	this->drop_percent =0;
	this->loadCoefficient =0;
	this->snapshot_loadCoefficient =0;
	this->effective_loadCoefficient =0;

	//ctrl-based load shedding
#ifdef _CTRL_LOAD_MANAGE_	
	ctrl_out_tuple_count =0;
	ctrl_num_of_queuing_tuples = 0;	
#endif	
	//end of part 1 of load manager, by Thao Pham

#endif
}

Output::~Output() {}

int Output::setInputQueue (Queue *inputQueue)
{
	ASSERT (inputQueue);

	this -> inputQueue = inputQueue;
	return 0;
}

int Output::setInStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> inStore = store;
	return 0;
}

int Output::setQueryOutput (Interface::QueryOutput *output)
{
	ASSERT (output);

	this -> output = output;
	return 0;
}

int Output::addAttr (Type type, unsigned int len, Column inCol)
{
	if (numAttrs == MAX_ATTRS)
		return -1;

	attrs [numAttrs].type = type;
	attrs [numAttrs].len = len;	
	inCols [numAttrs] = inCol;

	numAttrs ++;
	return 0;
}

int Output::initialize()
{
	int rc;

	ASSERT (output);
	ASSERT (numAttrs);

	if ((rc = output -> setNumAttrs (numAttrs)) != 0)
		return rc;

	for (unsigned int a = 0 ; a < numAttrs ; a++) {
		if ((rc = output -> setAttrInfo (a, attrs[a].type, attrs[a].len))
				!= 0)
			return rc;
	}

	if ((rc = output -> start()) != 0)
		return rc;

	// compute the offsets of the attributes
	unsigned int offset = DATA_OFFSET;
	for (unsigned int a = 0 ; a < numAttrs ; a++) {
		offsets [a] = offset;

		switch (attrs [a].type) {
		case INT:   offset += INT_SIZE;      break;
		case FLOAT: offset += FLOAT_SIZE;    break;
		case BYTE:  offset += BYTE_SIZE;     break;
		case CHAR:  offset += attrs [a].len; break;
		default:
			return -1;
		}
	}	
	tupleLen = offset;

	return 0;
}

//end of load manager, by Thao Pham		

int Output::run (TimeSlice timeSlice)
{

	if(status == START_PENDING)
		return run_in_start_pending(timeSlice);
	if(status ==START_PREPARING)
		return run_in_start_preparing(timeSlice);

	int rc;
	Element inputElement;
	Tuple inputTuple;
	unsigned int numElements;	
	char effect;

#ifdef _MONITOR_
	startTimer ();
#endif

	/*#ifdef _CTRL_LOAD_MANAGE_
	ctrl_out_tuple_count = 0;
#endif
	 */
	numElements = timeSlice;
	unsigned int e=0;

	//local stats computation, by Thao Pham
	Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();
	int n_before = num_tuples_processed;
	//end of part 1 of local stats computation, by Thao Pham

	//HR implementation by Lory Al Moakar
	if ( inputQueue ->isFull() ) {
		++n_full_queue;
		//printf("Size is %d \n",inputQueue->size());
	}
	// end of part 1 of HR implementation by LAM

	for (;e < numElements ; e++) {

		// Get the next element
		if (!inputQueue -> dequeue (inputElement))
			break;

		//local cost  computation, by Thao Pham (selectivity of output is always 1)
		// find out the num_tuples_processed so far
		num_tuples_processed += 1;
		// end of part 2 of local cost computation, by Thao Pham

		ASSERT (lastInputTs <= inputElement.timestamp);

#ifdef _DM_
		lastInputTs = inputElement.timestamp;
#endif

		// Ignore heartbeats
		if (inputElement.kind == E_HEARTBEAT)
			continue;

#ifdef _CTRL_LOAD_MANAGE_
		ctrl_num_of_queuing_tuples -=1;

#endif //_CTRL_LOAD_MANAGE		

		inputTuple = inputElement.tuple;

		// Output timestamp
		memcpy(buffer, &inputElement.timestamp, TIMESTAMP_SIZE);

		// Output effect: integer 1 for PLUS, integer 2 for MINUS
		effect = (inputElement.kind == E_PLUS)? '+' : '-';		
		memcpy(buffer + EFFECT_OFFSET, &effect, 1);

		// Output the remaining attributes
		for (unsigned int a = 0 ; a < numAttrs ; a++) {
			switch (attrs[a].type) {
			case INT:							
				memcpy (buffer + offsets[a],
						&(ICOL(inputTuple, inCols [a])),
						INT_SIZE);
				break;

			case FLOAT:
				memcpy (buffer + offsets[a],
						&(FCOL(inputTuple, inCols [a])),
						FLOAT_SIZE);
				break;

			case BYTE:
				buffer [offsets[a]] = BCOL(inputTuple,
						inCols[a]);
				break;

			case CHAR:

				strncpy (buffer + offsets [a],
						(CCOL(inputTuple, inCols [a])),
						attrs [a].len);

				break;

			default:
				return -1;
			}

		}

		UNLOCK_INPUT_TUPLE(inputTuple);

		// Response Time Calculation By Lory Al Moakar
		//get the current time
		Monitor::Timer mytime;
		unsigned long long int curTime = mytime.getTime(); 

		//calculate time elapsed
		curTime = curTime - this -> system_start_time;

		//calculate time elapsed in time units (in nano sec?)
		unsigned long long int time_elapsed = curTime - 
				((unsigned long long int)inputElement.timestamp *  (unsigned long long int)time_unit);
		//printf("%lld, %lld \n", curTime, inputElement.timestamp*1000);

		// Output the tuple
		if ((rc = output -> putNext (buffer, tupleLen,
				time_elapsed, time_unit )) != 0)
			return rc;
		//end of part 1 of response time calculation by LAM

		//response time monitor, by Thao Pham


		num_tuples_rt += 1;
		sum_response_time += time_elapsed;

		rt_last_ts = lastInputTs;
		if(rt_first_ts==0) rt_first_ts = lastInputTs;

		//end of part 2 of response time monitor, by Thao Pham

	}

#ifdef _MONITOR_
	stopTimer ();

#endif

	//local cost computation, by Thao Pham
	//get current time	
	unsigned long long int timeAfterLoop = mytime.getCPUTime();

	//calculate time elapsed during execution
	//add it to local cost
	if ( num_tuples_processed > n_before )//e >= 1  
		local_cost += timeAfterLoop - timeBeforeLoop;

	//end of part 3 of cost computation by Thao Pham

	//deactivate itself if no more inputs should be expected
	if(inputQueue->isEmpty()&&inputs[0]->status==INACTIVE)
		deactivate();
	return 0;
}
//HR implementation by Lory Al Moakar
/**
 * This method is used in order to calculate the local 
 * selectivity and the local cost per tuple of this operator
 * @return bool true or false
 * true when the statistics have changed and false otherwise
 */ 
bool Output::calculateLocalStats(){ 

	//implementation added by Thao Pham

	// no tuples were processed in the last round
	// --> no need to adjust costs and selectivity
	if ( num_tuples_processed == 0) {
		return false;
	}
	//calculate local cost
	//selectivity is always 1, as initialized
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

void Output::refreshPriority(){}
// end of part 1 of HR implementation by LAM

//HR with ready by Lory Al Moakar 
/** this method returns how ready this operator is to 
 * execute. If it returns a positive number --> ready
 * If it returns a zero or negative number --> not ready
 */
int Output::readyToExecute() {
	return inputQueue->size();
}

// end of part 1 of HR with ready by LAM

//average response time and response time moving trend monitor, by Thao Pham

/*compute response time based on statistics collects during the previous period
 * and reset the response time calculation cycle */

bool Output::computeAvgResponseTime()
{	
	if(num_tuples_rt==0) return false;//nothing change since the last period

	pre_avg_responsetime = avg_response_time;
	avg_response_time = sum_response_time/num_tuples_rt;

	double  period_length = (rt_last_ts-rt_first_ts)/2 - pre_rt_timestamp;
	rt_slope = (avg_response_time - pre_avg_responsetime)/period_length;

	pre_rt_timestamp = (rt_last_ts-rt_first_ts)/2;


	//debug only
	//printf("\n num tuples outputted: %d", num_tuples_rt);

	if(std_response_time>0)
	{
		if(avg_response_time < std_response_time ) //std response time is the smallest response time possible
			std_response_time = avg_response_time;
	}
	else 
		std_response_time = avg_response_time;

	//reset calculation cycle
	sum_response_time = 0;
	num_tuples_rt =0; 
	rt_first_ts = 0;

	return true;
}

/*check if accummulated delay observed?*/
bool Output::isAbnormalResponseTimeObserved()
{
	if(std_response_time ==0) return false;

	if(avg_response_time> (CONTROL_DELAY_TARGET + query_class_id*100000000))
		return true;

	else
		return false;     
}

bool Output::isIncreasing(double gap)
{
	/*if(std_response_time ==0) return false;

	if(avg_response_time> (1+factor)*CONTROL_DELAY_TARGET)//(delay_tolerance+0.2)*std_response_time)
		return true;

	else
		return false;
	 */
	if(rt_slope >= gap/100) return true;
	else return false;
}

bool Output::isAccummulatedDelayObserved(double systemCapacityExpandingFactor)
{
	if(std_response_time ==0) return false;
	//printf("%f\n", systemCapacityExpandingFactor);
	//if(avg_response_time> std_response_time + std_response_time/systemCapacityExpandingFactor)
	if(avg_response_time> std_response_time*systemCapacityExpandingFactor)
		return true;

	else
		return false;   

}

bool Output::isDecreasing(double gap)
{
	/*if(std_response_time ==0) return false;
	//printf("%f\n", systemCapacityExpandingFactor);
	//if(avg_response_time> std_response_time + std_response_time/systemCapacityExpandingFactor)
	if(avg_response_time < (1-factor)* CONTROL_DELAY_TARGET)
		return true;

	else
		return false;*/

	if(rt_slope <= -(gap/100)) return true;
	else return false;
}

bool Output::isSignificantlyBelowTargetObserved(double gap)
{
	if(std_response_time ==0) return false;
	//printf("%f\n", systemCapacityExpandingFactor);
	//if(avg_response_time> std_response_time + std_response_time/systemCapacityExpandingFactor)
	if(avg_response_time < (1-gap)* (CONTROL_DELAY_TARGET + query_class_id*100000000))
		return true;

	else
		return false;


}

bool Output::isCriticallyAbnormalResponseTimeObserved(double gap)
{
	if(std_response_time ==0) return false;

	if(avg_response_time> (1+gap)*(CONTROL_DELAY_TARGET + query_class_id*100000000))//(delay_tolerance+0.2)*std_response_time)
		return true;

	else
		return false;
}

//end of avg response time monitoring, by Thao Pham

//shedder embedded, by Thao Pham
int Output::run_with_shedder (TimeSlice timeSlice)
{
	int rc;
	Element inputElement;
	Tuple inputTuple;
	unsigned int numElements;
	char effect;

#ifdef _MONITOR_
	startTimer ();
#endif

	/*#ifdef _CTRL_LOAD_MANAGE_
		ctrl_out_tuple_count = 0;
	#endif
	 */
	numElements = timeSlice;
	unsigned int e=0;

	//local stats computation, by Thao Pham
	Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();
	int n_before = num_tuples_processed;
	//end of part 1 of local stats computation, by Thao Pham

	//HR implementation by Lory Al Moakar
	if ( inputQueue ->isFull() ) {
		++n_full_queue;
		//printf("Size is %d \n",inputQueue->size());
	}
	// end of part 1 of HR implementation by LAM

	for (;e < numElements ; e++) {

		// Get the next element
		if (!inputQueue -> dequeue (inputElement))
			break;


		// Ignore heartbeats
		if (inputElement.kind == E_HEARTBEAT)
			continue;

		//keep or drop?
		bool drop = 0;
		unsigned int r = rand()%100+1; //random number from 0-100
		if(r <= this->drop_percent){
			drop = 1;
		}

		if(!drop){ //keep
			num_tuples_processed += 1;
			// end of part 2 of local cost computation, by Thao Pham

			ASSERT (lastInputTs <= inputElement.timestamp);

#ifdef _DM_
			lastInputTs = inputElement.timestamp;
#endif
#ifdef _CTRL_LOAD_MANAGE_
			ctrl_num_of_queuing_tuples -=1;

#endif //_CTRL_LOAD_MANAGE

			inputTuple = inputElement.tuple;

			// Output timestamp
			memcpy(buffer, &inputElement.timestamp, TIMESTAMP_SIZE);

			// Output effect: integer 1 for PLUS, integer 2 for MINUS
			effect = (inputElement.kind == E_PLUS)? '+' : '-';
			memcpy(buffer + EFFECT_OFFSET, &effect, 1);

			// Output the remaining attributes
			for (unsigned int a = 0 ; a < numAttrs ; a++) {
				switch (attrs[a].type) {
				case INT:
					memcpy (buffer + offsets[a],
							&(ICOL(inputTuple, inCols [a])),
							INT_SIZE);
					break;

				case FLOAT:
					memcpy (buffer + offsets[a],
							&(FCOL(inputTuple, inCols [a])),
							FLOAT_SIZE);
					break;

				case BYTE:
					buffer [offsets[a]] = BCOL(inputTuple,
							inCols[a]);
					break;

				case CHAR:

					strncpy (buffer + offsets [a],
							(CCOL(inputTuple, inCols [a])),
							attrs [a].len);

					break;

				default:
					return -1;
				}

			}

			UNLOCK_INPUT_TUPLE(inputTuple);

			// Response Time Calculation By Lory Al Moakar
			//get the current time
			Monitor::Timer mytime;
			unsigned long long int curTime = mytime.getTime();

			//calculate time elapsed
			curTime = curTime - this -> system_start_time;

			//calculate time elapsed in time units (in nano sec?)
			unsigned long long int time_elapsed = curTime -
					((unsigned long long int)inputElement.timestamp *  (unsigned long long int)time_unit);
			//printf("%lld, %lld \n", curTime, inputElement.timestamp*1000);

			// Output the tuple
			if ((rc = output -> putNext (buffer, tupleLen,
					time_elapsed, time_unit )) != 0)
				return rc;
			//end of part 1 of response time calculation by LAM

			//response time monitor, by Thao Pham


			num_tuples_rt += 1;
			sum_response_time += time_elapsed;

			rt_last_ts = lastInputTs;
			if(rt_first_ts==0) rt_first_ts = lastInputTs;

			//end of part 2 of response time monitor, by Thao Pham

		}
		else { //drop
			UNLOCK_INPUT_TUPLE(inputElement.tuple);
		}

	}

#ifdef _MONITOR_
	stopTimer ();

#endif

	//local cost computation, by Thao Pham
	//get current time
	unsigned long long int timeAfterLoop = mytime.getCPUTime();

	//calculate time elapsed during execution
	//add it to local cost
	if ( num_tuples_processed > n_before )//e >= 1
		local_cost += timeAfterLoop - timeBeforeLoop;

	//end of part 3 of cost computation by Thao Pham

	return 0;
}


//end of shedder embedded by Thao Pham

//////////armaDILoS
int Output::run_in_start_pending(TimeSlice timeSlice){

	//in start_pending mode, an output op will store all output tuples to a queue instead of output to file
	int rc;
	Element inputElement;
	Tuple inputTuple;
	unsigned int numElements;
	char effect;

#ifdef _MONITOR_
	startTimer ();
#endif

	numElements = timeSlice;
	unsigned int e=0;

	//local stats computation, by Thao Pham
	Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();
	int n_before = num_tuples_processed;
	//end of part 1 of local stats computation, by Thao Pham

	//HR implementation by Lory Al Moakar
	if ( inputQueue ->isFull() ) {
		++n_full_queue;
		//printf("Size is %d \n",inputQueue->size());
	}
	// end of part 1 of HR implementation by LAM

	for (;e < numElements ; e++) {

		if(status == START_PREPARING){
			break;
		}
		// Get the next element
		if (!inputQueue -> dequeue (inputElement))
			break;

		//local cost  computation, by Thao Pham (selectivity of output is always 1)
		// find out the num_tuples_processed so far
		num_tuples_processed += 1;
		// end of part 2 of local cost computation, by Thao Pham

		ASSERT (lastInputTs <= inputElement.timestamp);

#ifdef _DM_
		lastInputTs = inputElement.timestamp;
#endif

		// Ignore heartbeats
		if (inputElement.kind == E_HEARTBEAT)
			continue;

#ifdef _CTRL_LOAD_MANAGE_
		ctrl_num_of_queuing_tuples -=1;

#endif //_CTRL_LOAD_MANAGE

		inputTuple = inputElement.tuple;

		// Output timestamp
		memcpy(buffer, &inputElement.timestamp, TIMESTAMP_SIZE);

		// Output effect: integer 1 for PLUS, integer 2 for MINUS
		effect = (inputElement.kind == E_PLUS)? '+' : '-';
		memcpy(buffer + EFFECT_OFFSET, &effect, 1);

		// Output the remaining attributes
		for (unsigned int a = 0 ; a < numAttrs ; a++) {
			switch (attrs[a].type) {
			case INT:
				memcpy (buffer + offsets[a],
						&(ICOL(inputTuple, inCols [a])),
						INT_SIZE);
				break;

			case FLOAT:
				memcpy (buffer + offsets[a],
						&(FCOL(inputTuple, inCols [a])),
						FLOAT_SIZE);
				break;

			case BYTE:
				buffer [offsets[a]] = BCOL(inputTuple,
						inCols[a]);
				break;

			case CHAR:

				strncpy (buffer + offsets [a],
						(CCOL(inputTuple, inCols [a])),
						attrs [a].len);

				break;

			default:
				return -1;
			}

		}

		UNLOCK_INPUT_TUPLE(inputTuple);

		//store output tuples in pending queue
		char* pending_tuple = new char[tupleLen];
		memcpy(pending_tuple,buffer, tupleLen);
		pending_tuples.push(pending_tuple);

		/*//no response time calculation or mornitoring now
		// Response Time Calculation By Lory Al Moakar
		//get the current time
		Monitor::Timer mytime;
		unsigned long long int curTime = mytime.getTime();

		//calculate time elapsed
		curTime = curTime - this -> system_start_time;

		//calculate time elapsed in time units (in nano sec?)
		unsigned long long int time_elapsed = curTime -
		  ((unsigned long long int)inputElement.timestamp *  (unsigned long long int)time_unit);
		//printf("%lld, %lld \n", curTime, inputElement.timestamp*1000);

		// Output the tuple
		if ((rc = output -> putNext (buffer, tupleLen,
						 time_elapsed, time_unit )) != 0)
			return rc;
		//end of part 1 of response time calculation by LAM

		//response time monitor, by Thao Pham


		num_tuples_rt += 1;
		sum_response_time += time_elapsed;

		rt_last_ts = lastInputTs;
		if(rt_first_ts==0) rt_first_ts = lastInputTs;

		//end of part 2 of response time monitor, by Thao Pham*/

	}

#ifdef _MONITOR_
	stopTimer ();

#endif

	//local cost computation, by Thao Pham
	//get current time
	unsigned long long int timeAfterLoop = mytime.getCPUTime();

	//calculate time elapsed during execution
	//add it to local cost
	if ( num_tuples_processed > n_before )//e >= 1
		local_cost += timeAfterLoop - timeBeforeLoop;

	//end of part 3 of cost computation by Thao Pham

	if(status==START_PREPARING)
		return run_in_start_preparing(numElements - e);
	return 0;
}

int Output::run_in_start_preparing(TimeSlice timeSlice){
	int rc;
	Element inputElement;
	Tuple inputTuple;
	unsigned int numElements;
	char effect;
	Timestamp tupleTs;

#ifdef _MONITOR_
	startTimer ();
#endif

	numElements = timeSlice;
	unsigned int e=0;

	//local stats computation, by Thao Pham
	Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();
	int n_before = num_tuples_processed;
	//end of part 1 of local stats computation, by Thao Pham

	//HR implementation by Lory Al Moakar
	if ( inputQueue ->isFull() ) {
		++n_full_queue;
		//printf("Size is %d \n",inputQueue->size());
	}
	// end of part 1 of HR implementation by LAM

	for (;e < numElements ; e++) {
		if(pending_tuples.size()>0){
			char* pending_tuple = pending_tuples.front();
			memcpy(buffer,pending_tuple,tupleLen);
			pending_tuples.pop();
			delete[]pending_tuple;
			memcpy (&tupleTs, buffer, TIMESTAMP_SIZE); //timestamp is at the begining of buffer
		}
		else{

			// Get the next element
			if (!inputQueue -> dequeue (inputElement))
				break;
			tupleTs = inputElement.timestamp;

			// Ignore heartbeats
			if (inputElement.kind == E_HEARTBEAT)
				continue;

			inputTuple = inputElement.tuple;

			// Output timestamp
			memcpy(buffer, &inputElement.timestamp, TIMESTAMP_SIZE);

			// Output effect: integer 1 for PLUS, integer 2 for MINUS
			effect = (inputElement.kind == E_PLUS)? '+' : '-';
			memcpy(buffer + EFFECT_OFFSET, &effect, 1);

			// Output the remaining attributes
			for (unsigned int a = 0 ; a < numAttrs ; a++) {
				switch (attrs[a].type) {
				case INT:
					memcpy (buffer + offsets[a],
							&(ICOL(inputTuple, inCols [a])),
							INT_SIZE);
					break;

				case FLOAT:
					memcpy (buffer + offsets[a],
							&(FCOL(inputTuple, inCols [a])),
							FLOAT_SIZE);
					break;

				case BYTE:
					buffer [offsets[a]] = BCOL(inputTuple,
							inCols[a]);
					break;

				case CHAR:

					strncpy (buffer + offsets [a],
							(CCOL(inputTuple, inCols [a])),
							attrs [a].len);

					break;

				default:
					return -1;
				}

			}

			UNLOCK_INPUT_TUPLE(inputTuple);
		}

		//local cost  computation, by Thao Pham (selectivity of output is always 1)
		// find out the num_tuples_processed so far
		num_tuples_processed += 1;
		// end of part 2 of local cost computation, by Thao Pham

		ASSERT (lastInputTs <= tupleTs);

#ifdef _DM_
		lastInputTs = tupleTs;
#endif



#ifdef _CTRL_LOAD_MANAGE_
		ctrl_num_of_queuing_tuples -=1;

#endif //_CTRL_LOAD_MANAGE


		// Response Time Calculation By Lory Al Moakar
		//get the current time
		Monitor::Timer mytime;
		unsigned long long int curTime = mytime.getTime();

		//calculate time elapsed
		curTime = curTime - this -> system_start_time;

		//calculate time elapsed in time units (in nano sec?)
		unsigned long long int time_elapsed = curTime -
				((unsigned long long int)tupleTs *  (unsigned long long int)time_unit);
		//printf("%lld, %lld \n", curTime, inputElement.timestamp*1000);

		// Output the tuple
		if ((rc = output -> putNext (buffer, tupleLen,
				time_elapsed, time_unit )) != 0)
			return rc;
		//end of part 1 of response time calculation by LAM

		//response time monitor, by Thao Pham


		num_tuples_rt += 1;
		sum_response_time += time_elapsed;

		rt_last_ts = lastInputTs;
		if(rt_first_ts==0) rt_first_ts = lastInputTs;

		//end of part 2 of response time monitor, by Thao Pham

	}

#ifdef _MONITOR_
	stopTimer ();

#endif

	//local cost computation, by Thao Pham
	//get current time
	unsigned long long int timeAfterLoop = mytime.getCPUTime();

	//calculate time elapsed during execution
	//add it to local cost
	if ( num_tuples_processed > n_before )//e >= 1
		local_cost += timeAfterLoop - timeBeforeLoop;

	//end of part 3 of cost computation by Thao Pham

	return 0;
}

void Output::deactivate(){

	cout<< "hey I am an output finishing" <<endl;
	status = INACTIVE;
	Element e;
	while(!inputQueue->isEmpty()){
		inputQueue->dequeue(e);
		UNLOCK_INPUT_TUPLE(e.tuple);
	}
	pthread_mutex_lock(mutex_outputIDs);
	outputIDs->insert(this->id);
	pthread_mutex_unlock(mutex_outputIDs);
resetLocalStatisticsComputationCycle();
}
