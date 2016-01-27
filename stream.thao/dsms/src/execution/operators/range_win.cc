/**
 * @file       range_win.cc
 * @date       May 30, 2004
 * @brief      Range window operator
 */


#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _RANGE_WIN_
#include "execution/operators/range_win.h"
#endif
//HR implementation by Lory Al Moakar
#ifndef _COMPAREOPS_
#include "execution/operators/compareOPS.h"
#endif

#include <queue>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
//end of part 0 of HR implementation by LAM

using namespace Execution;
using namespace std;

#define LOCK_INPUT_TUPLE(t) (inStore -> addRef ((t)))
#define UNLOCK_INPUT_TUPLE(t) (inStore -> decrRef ((t)))

RangeWindow::RangeWindow(unsigned int id, std::ostream &_LOG)
: LOG (_LOG)
{
	this -> id               = id;
	this -> inputQueue       = 0;
	this -> outputQueue      = 0;
	this -> winSynopsis      = 0;
	this -> windowSize       = 0;
	this -> inStore          = 0;
	this -> bStalled         = false;
	this -> lastInputTs      = 0;
	this -> lastOutputTs     = 0;

	//HR implementation by Lory Al Moakar
	//initialize operator type and statistics 

	this -> operator_type = RANGE_WIN;
	this -> mean_cost = 1;
	this -> mean_selectivity = 1;
	this -> local_selectivity =1;
	this -> n_tuples_outputted = 0;
	this -> local_cost = 1;
	this -> priority = 1;
	this -> num_tuples_processed = 0;
	this -> firstRefresh = true;
	this -> interarrivaltime = 0;

	//end of part 1 of HR implementation by LAM

	//load manager, by Thao Pham
	this->snapshot_local_cost_per_tuple = 0;
	this-> isShedder = false;
	this->input_load =0; //when isShedder is true, this operator is not a source load and therefore its input load is the input rate at the corresponding source * (product of all preceding ops)
	this->drop_percent =0;
	this->loadCoefficient =0;
	this->snapshot_loadCoefficient =0;
	this->effective_loadCoefficient =0;
	//end of load manager, by Thao Pham
}

RangeWindow::~RangeWindow() {}

int RangeWindow::setInputQueue (Queue *inputQueue) 
{
	ASSERT (inputQueue);

	this -> inputQueue = inputQueue;
	return 0;
}

int RangeWindow::setOutputQueue (Queue *outputQueue)
{
	ASSERT (outputQueue);

	this -> outputQueue = outputQueue;
	return 0;
}

int RangeWindow::setWindowSize (unsigned int windowSize)
{
	ASSERT (windowSize > 0);

	this -> windowSize = windowSize;
	return 0;
}

int RangeWindow::setWindowSynopsis (WindowSynopsis *winSynopsis) 
{
	ASSERT (winSynopsis);

	this -> winSynopsis = winSynopsis;
	return 0;
}

int RangeWindow::setInStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> inStore = store;
	return 0;
}

int RangeWindow::run(TimeSlice timeSlice)
{
	if(status == STOP_PREPARING)
		return run_in_stop_preparing(timeSlice);
	int rc;
	unsigned int numElements;
	Element      inputElement;
	Tuple        inputTuple;
	Element      outputElement;

#ifdef _MONITOR_
	startTimer ();
#endif							

	//HR implementation by Lory Al Moakar
	//get current time
	Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();

	//end of part 2 of HR implementation by LAM

	// We are stalled ...
	if (bStalled) {

		// we try clearing it ...
		if ((rc = clearStall()) != 0)
			return rc;

		// and return if we fail to.
		if (bStalled) {
#ifdef _MONITOR_
			stopTimer ();
			logOutTs (lastOutputTs);
#endif										
			return 0;
		}
		//HR implementation by Lory Al Moakar
		// find out the num_tuples_processed so far
		//num_tuples_processed +=  1;
		// end of part 3 of HR implementation by LAM	
	}

	numElements = timeSlice;
	unsigned int e = 0;

	for ( ; e < numElements ; e++) {		

		// Get the next element
		if (!inputQueue -> dequeue (inputElement)){
			break;
		}

		//HR implementation by Lory Al Moakar
		// find out the num_tuples_processed so far
		num_tuples_processed +=  1;
		// end of part 3 of HR implementation by LAM

		// We should not get MINUS tuples in the input
		ASSERT (inputElement.kind != E_MINUS);

		// Input should be timestamp ordered
		ASSERT (lastInputTs <= inputElement.timestamp);

		//HR implementation by Lory Al Moakar
		//maintaining the interarrival time
		interarrivaltime += inputElement.timestamp - lastInputTs ;

		//printf( "TS is %d last input ts is %d op is %d n_tuples is %d\n",  
		//	inputElement.timestamp, lastInputTs, operator_id,
		//	num_tuples_processed);
		// end of part xx of HR implementation by LAM


		lastInputTs = inputElement.timestamp;
		inputTuple = inputElement.tuple;

		if (inputElement.kind == E_PLUS) {




			LOCK_INPUT_TUPLE (inputElement.tuple);

			if ((rc = winSynopsis -> insertTuple (inputTuple,
					lastInputTs)) != 0)
				return rc;

			// Expire tuples with timestamp <= (lastInputTs - windowSize)
			if (lastInputTs >= windowSize)
				if ((rc = expireTuples (lastInputTs - windowSize)) != 0)
					return rc;

			// expireTuple method sets bStalled flag if it stalled when
			// sending out MINUS expired tuples
			if (bStalled) {
				stalledElement = inputElement;
				printf("BLOCKED RANGE WIN\n");
#ifdef _MONITOR_
				stopTimer ();
				logOutTs (lastOutputTs);
#endif						
				//HR implementation by Lory Al Moakar	 
				break;//return 0;
				// end of part 5 of HR implementation by LAM
			}

			// We are blocked
			if (outputQueue -> isFull()) {
				printf("BLOCKED RANGE WIN\n");
				bStalled = true;
				stalledElement = inputElement;

#ifdef _MONITOR_
				stopTimer ();
				logOutTs (lastOutputTs);
#endif					

				//HR implementation by Lory Al Moakar
				break; //return 0;
				// end of part 5 of HR implementation by LAM
			}

			// Generate the output element
			outputElement.kind      = E_PLUS;
			outputElement.timestamp = inputElement.timestamp;
			outputElement.tuple     = inputTuple;

			outputQueue -> enqueue (outputElement);

			//HR implementation by Lory Al Moakar
			// increment the number of tuples outputted so far
			n_tuples_outputted +=  1;
			// end of part 5 of HR implementation by LAM

			lastOutputTs = outputElement.timestamp;						
		}

		else {

			// Expire tuples with timestamp < (lastInputTs - windowSize)
			if (lastInputTs >= windowSize)
				if ((rc = expireTuples (lastInputTs - windowSize)) != 0)
					return rc;

			// expireTuple method sets bStalled flag if it stalled when
			// sending out MINUS expired tuples
			if (bStalled) {
				stalledElement = inputElement;

#ifdef _MONITOR_
				stopTimer ();
				logOutTs (lastOutputTs);
#endif							
				//HR implementation by Lory Al Moakar
				break; //return 0;
				// end of part 5 of HR implementation by LAM		 

			}	
		}


	}

	//ASSERT (!bStalled);	
	if (!outputQueue -> isFull() && (lastOutputTs < lastInputTs)) {
		outputQueue -> enqueue (Element::Heartbeat(lastInputTs));
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 5 of HR implementation by LAM
	}

#ifdef _MONITOR_
	stopTimer ();
	logOutTs (lastOutputTs);
#endif							

	//HR implementation by Lory Al Moakar
	//get current time	
	unsigned long long int timeAfterLoop = mytime.getCPUTime();

	//calculate time elapsed during execution
	//add it to local cost
	if ( e > 0 )
		local_cost += timeAfterLoop - timeBeforeLoop;

	//deactivate itself it there is no more incoming tuples to expect
	if(inputQueue->isEmpty()&&inputs[0]->status==INACTIVE)
		deactivate();
	return 0;
}

int RangeWindow::clearStall() {
	int rc; Element outputElement;

	ASSERT (bStalled);

	bStalled = false;
	if (lastInputTs >= windowSize)
		if ((rc = expireTuples (lastInputTs - windowSize)) != 0)
			return rc;

	// We stalled yet again trying to expire tuples
	if (bStalled)
		return 0;

	// Nothing more to do for heartbeats
	if (stalledElement.kind == E_HEARTBEAT)
		return 0;

	if (outputQueue -> isFull()) {
		bStalled = true;
		return 0;
	}

	outputElement.kind         = stalledElement.kind;
	outputElement.timestamp    = stalledElement.timestamp;
	outputElement.tuple        = stalledElement.tuple;

	outputQueue -> enqueue (outputElement);
	//HR implementation by Lory Al Moakar
	// increment the number of tuples outputted so far
	n_tuples_outputted +=  1;
	// end of part 6 of HR implementation by LAM
	lastOutputTs = outputElement.timestamp;

	return 0;
}

int RangeWindow::expireTuples (Timestamp expTs)
{
	int rc;
	Tuple     oldestTuple;
	Timestamp oldestTupleTs;
	Element   outputElement;

	while (true) {	
		ASSERT(!bStalled);

		// Window is empty: no tuples to expire
		if (winSynopsis -> isEmpty())
			return 0;

		// If the oldest tuple has a timestamp <= expTs, delete it from
		// the window and send a MINUS tuple in the output queue
		rc = winSynopsis -> getOldestTuple (oldestTuple, oldestTupleTs);		
		if (rc != 0) return rc;

		// No tuples to expire
		if (oldestTupleTs > expTs)
			return 0;

		// Output queue is full, we cannot send the MINUS tuple
		if (outputQueue -> isFull()) {
			bStalled = true;
			return 0; 
		}

		// Construct the element corresponding to the MINUS tuples, and
		// send it.
		outputElement.kind      = E_MINUS;
		outputElement.tuple     = oldestTuple;
		outputElement.timestamp = oldestTupleTs + windowSize;

		outputQueue -> enqueue (outputElement);

		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 7 of HR implementation by LAM

		lastOutputTs = outputElement.timestamp;

		winSynopsis -> deleteOldestTuple();

	}

	return 0;
}

//HR implementation by Lory Al Moakar
/**
 * This method is used in order to calculate the local 
 * selectivity and the local cost per tuple of this operator
 * @return bool true or false
 * true when the statistics have changed and false otherwise
 */ 
bool RangeWindow::calculateLocalStats() 
{
	// no tuples were processed in the last round
	// --> no need to adjust costs and selectivity
	if ( num_tuples_processed == 0){
		return false;
	}

	interarrivaltime = interarrivaltime/(double) num_tuples_processed;
	//printf ( "interarrivaltime %f op %d num_tuples_processed %d \n", interarrivaltime,
	//	 operator_id, num_tuples_processed);
	n_tuples_win = (windowSize/ interarrivaltime);

	//printf ( "n_tuples_in_win %f t %f op %d\n", n_tuples_win, interarrivaltime,
	//		 operator_id);
	interarrivaltime = 0;
	//calculate local selectivity & cost
	if (!firstRefresh) {

		local_selectivity = (1-ALPHA) * local_selectivity +
				ALPHA * (double) n_tuples_outputted /(double) num_tuples_processed;

		/*printf ( "local_selectivity %f n_tuples_outputted %d num_tuples_processed %d \n",
	     local_selectivity, n_tuples_outputted,  num_tuples_processed ) ;*/
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

		if ( (current_avg - avg_k) > OULIER_FACTOR * stdev) {
			//      printf("OUTLIER current %f  new avg %f old avg %f \n", current_avg, local_cost_per_tuple,
			//     old_local_cost);
			local_cost_per_tuple  = old_local_cost;

		}

		//printf ( "RANGEWIN OPERATOR %d costs %llu  n_tuples %d smoothed %f \n",
		//     operator_id, local_cost, num_tuples_processed,local_cost_per_tuple);

	}
	else {
		if ( num_tuples_processed < 10) {
			return false;
		}
		local_selectivity = (double) n_tuples_outputted / (double) num_tuples_processed;

		local_cost_per_tuple = (double) local_cost / (double) num_tuples_processed; // /time_unit;
		M_k = local_cost_per_tuple;
		avg_k = local_cost_per_tuple;
		M_k_1 = M_k;
		S_k = 0;
		k_measurements = 1;
		//printf ( "OPERATOR %d costs %llu  n_tuples %d\n",
		//	     operator_id, local_cost, num_tuples_processed);
	}

	//printf("inside rangeWin: local_cost_per_tuple %f, selectivity %f, tuples_processed %d, output %d\n",
	//local_cost_per_tuple, local_selectivity, num_tuples_processed,
	//n_tuples_outputted);
	firstRefresh = false;

	//load manager, by Thao Pham
	snapshot_local_cost_per_tuple = (double) local_cost / (double) num_tuples_processed;
	//end of load manager, by Thao Pham
	return true;
}


/** this method is used in order to calculate the priority
 * of the operator. This is used in order to assign the priorities
 * used by the scheduler to schedule operators.
 */

void RangeWindow::refreshPriority()
{
	mean_cost = local_cost_per_tuple;
	mean_selectivity = 0;
	//if I have not refreshed my stats -->
	// no priority to refresh
	if ( firstRefresh )
		return;
	//is this operator a shared operator
	if ( numOutputs != 1 ) {
		mean_selectivity = 0;
		//used for PDT
		priority_queue<Operator*,vector<Operator*>, CompareOPS> mypq;

		for ( int i = 0; i< numOutputs; i++ ) {

			//IMPLEMENTING PDT
			mypq.push(outputs[i]);

		}//end of for loop
		double new_priority = 0.0;
		double old_priority = 0.0;
		double new_mean_selectivity = 0;

		double new_mean_cost = local_cost_per_tuple;

		for ( int i=0; i < numOutputs; i++ ) {
			Operator * top_queue = mypq.top();
			//printf("top queue %d %f \n " , i, top_queue->priority );
			mypq.pop();
			// if I am outputting tuples to the user
			// along one of my paths
			//--> I should only add up my selectivity
			if ( top_queue->operator_type == OUTPUT ||
					top_queue->operator_type == SINK ||
					top_queue->firstRefresh == true){
				new_mean_selectivity += local_selectivity;

			}
			else if ( top_queue->operator_type == BIN_JOIN ||
					top_queue->operator_type == BIN_STR_JOIN) {
				//we do not get here unless this operator is a window operator
				// since only window operators output to joins
				//my output is a join
				int j = 0;
				if ( top_queue->inputs[0]->operator_id ==operator_id)
					j = 1;
				double total_mean_costs = 0;
				for ( int k = 0; k < top_queue->numOutputs; k++) {
					if (  top_queue->outputs[k]-> mean_cost > 0 )
						total_mean_costs+= top_queue->outputs[k]-> mean_cost;
				}
				new_mean_cost += local_selectivity * top_queue->local_cost_per_tuple +
						(local_selectivity *
								top_queue->local_selectivity *
								top_queue->inputs[j]->local_selectivity
								* top_queue->inputs[j]->n_tuples_win
								* total_mean_costs);

				new_mean_selectivity += local_selectivity * top_queue->mean_selectivity
						* top_queue->inputs[j]->local_selectivity
						* top_queue->inputs[j]->n_tuples_win;

			} //end of this operator is a join
			else {
				new_mean_cost += top_queue->mean_cost*local_selectivity;
				new_mean_selectivity +=  local_selectivity *
						top_queue->mean_selectivity;
			}

			new_priority = new_mean_selectivity / new_mean_cost;
			if ( new_priority < old_priority )
				break;
			mean_selectivity = new_mean_selectivity;
			mean_cost = new_mean_cost;
			old_priority = new_priority;

		} // end of for loop

	}//end of if the operator is shared
	else {
		// if I am the last operator along the path
		// then my mean selectivity and mean cost
		// are equal to my local ones.
		//  printf("I AM NOT SHARED\n");
		if ( outputs[0]->operator_type == OUTPUT ||
				outputs[0]->operator_type == SINK ||
				outputs[0]->firstRefresh == true){
			mean_cost = local_cost_per_tuple;
			mean_selectivity = local_selectivity;
			//printf ("pkkkkkkkkt");
		}

		// is its output a join ?
		//its output is  a join
		else if ( outputs[0]->operator_type == BIN_JOIN ||
				outputs[0]->operator_type == BIN_STR_JOIN )
		{
			//we do not get here unless this operator is a window operator
			// since only window operators output to joins
			//my output is a join

			int j = 0;
			if ( outputs[0]->inputs[0]->operator_id ==operator_id)
				j = 1;
			double total_mean_costs = 0;
			for ( int k = 0; k < outputs[0]->numOutputs; k++)	{
				if ( outputs[0]->outputs[k]-> mean_cost > 0 )
					total_mean_costs+=outputs[0]->outputs[k]-> mean_cost;
			}
			//printf("MY output is a join lala %f \n", total_mean_costs);
			mean_cost = local_cost_per_tuple +
					(local_selectivity * outputs[0]->local_cost_per_tuple ) +
					(local_selectivity * outputs[0]->local_selectivity
							* outputs[0]->inputs[j]->local_selectivity
							* outputs[0]->inputs[j]->n_tuples_win
							* total_mean_costs);
			//printf( "MY mean cost :  part1 %f part2 %f ",
			//      local_cost_per_tuple,(local_selectivity * outputs[0]->local_cost_per_tuple));
			mean_selectivity = local_selectivity *outputs[0]->mean_selectivity
					* outputs[0]->inputs[j]->local_selectivity
					* outputs[0]->inputs[j]->n_tuples_win;

			//printf( "%d  Peer %d stats: Si %f wi %d join mean si %f \n",operator_id,
			//      outputs[0]->inputs[j]->operator_id,
			//  outputs[0]->inputs[j]->local_selectivity,
			//  outputs[0]->inputs[j]->n_tuples_win,
			//  outputs[0]->mean_selectivity);
		} //end of else if join
		else {
			//printf("NOt outputting to a join yay %d", outputs[0]->operator_type);
			mean_selectivity = local_selectivity *
					outputs[0]->mean_selectivity;

			mean_cost = outputs[0]->mean_cost *
					local_selectivity + mean_cost;

		} //end of else
		//printf("DONE");
	}//end of if not shared

	//calculate priority
	priority = mean_selectivity / mean_cost;

	//printf("mean selectivity %f mean_cost %f priority %f\n",
	//mean_selectivity,  mean_cost, priority );
	//printf ( "selectivity %d %f %f \n", operator_id,
	//   mean_selectivity, mean_cost);

	//reset stats
	num_tuples_processed= 0;
	n_tuples_outputted = 0;
	local_cost = 0;



}
// end of part 13 of HR implementation by LAM
//HR with ready by Lory Al Moakar 
/** this method returns how ready this operator is to 
 * execute. If it returns a positive number --> ready
 * If it returns a zero or negative number --> not ready
 */
int RangeWindow::readyToExecute() {
	if ( outputQueue->isFull() ) {
		printf("BLOCKED RANGE WIN\n");
		return 0;
	}
	return inputQueue->size();
}

// end of part 1 of HR with ready by LAM

int RangeWindow::run_with_shedder(TimeSlice timeSlice)
{
	int rc;
	unsigned int numElements;
	Element      inputElement;
	Tuple        inputTuple;
	Element      outputElement;

#ifdef _MONITOR_
	startTimer ();
#endif

	//HR implementation by Lory Al Moakar
	//get current time
	Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();

	//end of part 2 of HR implementation by LAM

	// We are stalled ...
	if (bStalled) {

		// we try clearing it ...
		if ((rc = clearStall()) != 0)
			return rc;

		// and return if we fail to.
		if (bStalled) {
#ifdef _MONITOR_
			stopTimer ();
			logOutTs (lastOutputTs);
#endif
			return 0;
		}
		//HR implementation by Lory Al Moakar
		// find out the num_tuples_processed so far
		//num_tuples_processed +=  1;
		// end of part 3 of HR implementation by LAM
	}

	numElements = timeSlice;
	unsigned int e = 0;

	for ( ; e < numElements ; e++) {

		// Get the next element
		if (!inputQueue -> dequeue (inputElement)){
			break;
		}

		//drop or keep
		bool drop = 0;
		unsigned int r = rand()%100+1; //random number from 0-100

		if(r <= this->drop_percent){
			drop = 1;
		}
		if(!drop){
			//HR implementation by Lory Al Moakar
			// find out the num_tuples_processed so far
			num_tuples_processed +=  1;
			// end of part 3 of HR implementation by LAM

			// We should not get MINUS tuples in the input
			ASSERT (inputElement.kind != E_MINUS);

			// Input should be timestamp ordered
			ASSERT (lastInputTs <= inputElement.timestamp);

			//HR implementation by Lory Al Moakar
			//maintaining the interarrival time
			interarrivaltime += inputElement.timestamp - lastInputTs ;

			//printf( "TS is %d last input ts is %d op is %d n_tuples is %d\n",
			//	inputElement.timestamp, lastInputTs, operator_id,
			//	num_tuples_processed);
			// end of part xx of HR implementation by LAM


			lastInputTs = inputElement.timestamp;
			inputTuple = inputElement.tuple;

			if (inputElement.kind == E_PLUS) {




				LOCK_INPUT_TUPLE (inputElement.tuple);

				if ((rc = winSynopsis -> insertTuple (inputTuple,
						lastInputTs)) != 0)
					return rc;

				// Expire tuples with timestamp <= (lastInputTs - windowSize)
				if (lastInputTs >= windowSize)
					if ((rc = expireTuples (lastInputTs - windowSize)) != 0)
						return rc;

				// expireTuple method sets bStalled flag if it stalled when
				// sending out MINUS expired tuples
				if (bStalled) {
					stalledElement = inputElement;
					printf("BLOCKED RANGE WIN\n");
#ifdef _MONITOR_
					stopTimer ();
					logOutTs (lastOutputTs);
#endif
					//HR implementation by Lory Al Moakar
					break;//return 0;
					// end of part 5 of HR implementation by LAM
				}

				// We are blocked
				if (outputQueue -> isFull()) {
					printf("BLOCKED RANGE WIN\n");
					bStalled = true;
					stalledElement = inputElement;

#ifdef _MONITOR_
					stopTimer ();
					logOutTs (lastOutputTs);
#endif

					//HR implementation by Lory Al Moakar
					break; //return 0;
					// end of part 5 of HR implementation by LAM
				}

				// Generate the output element
				outputElement.kind      = E_PLUS;
				outputElement.timestamp = inputElement.timestamp;
				outputElement.tuple     = inputTuple;

				outputQueue -> enqueue (outputElement);

				//HR implementation by Lory Al Moakar
				// increment the number of tuples outputted so far
				n_tuples_outputted +=  1;
				// end of part 5 of HR implementation by LAM

				lastOutputTs = outputElement.timestamp;
			}

			else {

				// Expire tuples with timestamp < (lastInputTs - windowSize)
				if (lastInputTs >= windowSize)
					if ((rc = expireTuples (lastInputTs - windowSize)) != 0)
						return rc;

				// expireTuple method sets bStalled flag if it stalled when
				// sending out MINUS expired tuples
				if (bStalled) {
					stalledElement = inputElement;

#ifdef _MONITOR_
					stopTimer ();
					logOutTs (lastOutputTs);
#endif
					//HR implementation by Lory Al Moakar
					break; //return 0;
					// end of part 5 of HR implementation by LAM

				}
			}
		}
		else{//drop
			UNLOCK_INPUT_TUPLE (inputElement.tuple);
		}

	}

	//ASSERT (!bStalled);
	if (!outputQueue -> isFull() && (lastOutputTs < lastInputTs)) {
		outputQueue -> enqueue (Element::Heartbeat(lastInputTs));
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 5 of HR implementation by LAM
	}

#ifdef _MONITOR_
	stopTimer ();
	logOutTs (lastOutputTs);
#endif

	//HR implementation by Lory Al Moakar
	//get current time
	unsigned long long int timeAfterLoop = mytime.getCPUTime();

	//calculate time elapsed during execution
	//add it to local cost
	if ( e > 0 )
		local_cost += timeAfterLoop - timeBeforeLoop;

	return 0;
}

//ArMaDILoS
void RangeWindow::deactivate(){
	//deactivate itself first
	status = INACTIVE;
	bStalled = false;
	//empty the input queue
	Element e;
	while(!inputQueue->isEmpty()){
		inputQueue->dequeue(e);
		if(e.tuple){
			UNLOCK_INPUT_TUPLE(e.tuple);
		}
	}
	//clear the synopsis
	while(!winSynopsis->isEmpty()){
		Tuple oldestTuple;
		Timestamp ts;
		winSynopsis->getOldestTuple(oldestTuple,ts);
		winSynopsis->deleteOldestTuple();
		UNLOCK_INPUT_TUPLE(oldestTuple);
	}
	if(outputQueue->isEmpty()){
		for(int i=0;i<numOutputs;i++){
			outputs[i]->deactivate();
		}
	}
	resetLocalStatisticsComputationCycle();
	local_cost_per_tuple = 0;
}

int RangeWindow::run_in_stop_preparing(TimeSlice timeSlice){

	int rc;
	unsigned int numElements;
	Element      inputElement;
	Tuple        inputTuple;
	Element      outputElement;

#ifdef _MONITOR_
	startTimer ();
#endif

	//HR implementation by Lory Al Moakar
	//get current time
	Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();

	//end of part 2 of HR implementation by LAM

	// We are stalled ...
	if (bStalled) {

		// we try clearing it ...
		if ((rc = clearStallInStopPreparing()) != 0)
			return rc;
		//if the operator has been deactivated
		if(status ==INACTIVE)
			return 0;

		// and return if we fail to.
		if (bStalled) {
#ifdef _MONITOR_
			stopTimer ();
			logOutTs (lastOutputTs);
#endif
			return 0;
		}
		//HR implementation by Lory Al Moakar
		// find out the num_tuples_processed so far
		//num_tuples_processed +=  1;
		// end of part 3 of HR implementation by LAM
	}

	numElements = timeSlice;
	unsigned int e = 0;

	Tuple oldestTuple;
	Timestamp oldestTupleTs;

	for ( ; e < numElements ; e++) {

		rc = winSynopsis -> getOldestTuple (oldestTuple, oldestTupleTs);
		if(oldestTupleTs >=stopTupleTs){
			deactivatePrecedingOps();
			return 0;
		}

		// Get the next element
		if (!inputQueue -> dequeue (inputElement)){
			break;
		}

		//HR implementation by Lory Al Moakar
		// find out the num_tuples_processed so far
		num_tuples_processed +=  1;
		// end of part 3 of HR implementation by LAM

		// We should not get MINUS tuples in the input
		ASSERT (inputElement.kind != E_MINUS);

		// Input should be timestamp ordered
		ASSERT (lastInputTs <= inputElement.timestamp);

		//HR implementation by Lory Al Moakar
		//maintaining the interarrival time
		interarrivaltime += inputElement.timestamp - lastInputTs ;

		//printf( "TS is %d last input ts is %d op is %d n_tuples is %d\n",
		//	inputElement.timestamp, lastInputTs, operator_id,
		//	num_tuples_processed);
		// end of part xx of HR implementation by LAM


		lastInputTs = inputElement.timestamp;
		inputTuple = inputElement.tuple;

		if (inputElement.kind == E_PLUS) {




			LOCK_INPUT_TUPLE (inputElement.tuple);

			if ((rc = winSynopsis -> insertTuple (inputTuple,
					lastInputTs)) != 0)
				return rc;

			// Expire tuples with timestamp <= (lastInputTs - windowSize)
			if (lastInputTs >= windowSize)
				if ((rc = expireTuplesInStopPreparing(lastInputTs - windowSize)) != 0)
					return rc;
			//if this operator has been turned of...
			if(status == INACTIVE)
				break;

			// expireTuple method sets bStalled flag if it stalled when
			// sending out MINUS expired tuples
			if (bStalled) {
				stalledElement = inputElement;
				printf("BLOCKED RANGE WIN\n");
#ifdef _MONITOR_
				stopTimer ();
				logOutTs (lastOutputTs);
#endif
				//HR implementation by Lory Al Moakar
				break;//return 0;
				// end of part 5 of HR implementation by LAM
			}

			// We are blocked
			if (outputQueue -> isFull()) {
				printf("BLOCKED RANGE WIN\n");
				bStalled = true;
				stalledElement = inputElement;

#ifdef _MONITOR_
				stopTimer ();
				logOutTs (lastOutputTs);
#endif

				//HR implementation by Lory Al Moakar
				break; //return 0;
				// end of part 5 of HR implementation by LAM
			}

			// Generate the output element
			outputElement.kind      = E_PLUS;
			outputElement.timestamp = inputElement.timestamp;
			outputElement.tuple     = inputTuple;

			outputQueue -> enqueue (outputElement);

			//HR implementation by Lory Al Moakar
			// increment the number of tuples outputted so far
			n_tuples_outputted +=  1;
			// end of part 5 of HR implementation by LAM

			lastOutputTs = outputElement.timestamp;
		}

		else {

			// Expire tuples with timestamp < (lastInputTs - windowSize)
			if (lastInputTs >= windowSize)
				if ((rc = expireTuplesInStopPreparing(lastInputTs - windowSize)) != 0)
					return rc;
			if(status==INACTIVE)
				break;

			// expireTuple method sets bStalled flag if it stalled when
			// sending out MINUS expired tuples
			if (bStalled) {
				stalledElement = inputElement;

#ifdef _MONITOR_
				stopTimer ();
				logOutTs (lastOutputTs);
#endif
				//HR implementation by Lory Al Moakar
				break; //return 0;
				// end of part 5 of HR implementation by LAM

			}
		}


	}

	//ASSERT (!bStalled);
	if (!outputQueue -> isFull() && (lastOutputTs < lastInputTs)) {
		outputQueue -> enqueue (Element::Heartbeat(lastInputTs));
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 5 of HR implementation by LAM
	}

#ifdef _MONITOR_
	stopTimer ();
	logOutTs (lastOutputTs);
#endif

	//HR implementation by Lory Al Moakar
	//get current time
	unsigned long long int timeAfterLoop = mytime.getCPUTime();

	//calculate time elapsed during execution
	//add it to local cost
	if ( e > 0 )
		local_cost += timeAfterLoop - timeBeforeLoop;
	return 0;
}
int RangeWindow::expireTuplesInStopPreparing(Timestamp expTs){
	int rc;
	Tuple     oldestTuple;
	Timestamp oldestTupleTs;
	Element   outputElement;

	while (true) {
		ASSERT(!bStalled);

		// Window is empty: no tuples to expire
		if (winSynopsis -> isEmpty())
			return 0;

		// If the oldest tuple has a timestamp <= expTs, delete it from
		// the window and send a MINUS tuple in the output queue
		rc = winSynopsis -> getOldestTuple (oldestTuple, oldestTupleTs);
		if (rc != 0) return rc;

		// No tuples to expire
		if (oldestTupleTs > expTs)
			return 0;

		// Output queue is full, we cannot send the MINUS tuple
		if (outputQueue -> isFull()) {
			bStalled = true;
			return 0;
		}

		// Construct the element corresponding to the MINUS tuples, and
		// send it.
		outputElement.kind      = E_MINUS;
		outputElement.tuple     = oldestTuple;
		outputElement.timestamp = oldestTupleTs + windowSize;

		outputQueue -> enqueue (outputElement);

		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 7 of HR implementation by LAM

		lastOutputTs = outputElement.timestamp;

		winSynopsis -> deleteOldestTuple();

	}

	return 0;
}

int RangeWindow::clearStallInStopPreparing(){

	int rc; Element outputElement;
	ASSERT (bStalled);

	bStalled = false;
	if (lastInputTs >= windowSize)
		if ((rc = expireTuplesInStopPreparing(lastInputTs - windowSize)) != 0)
			return rc;

	// We stalled yet again trying to expire tuples
	if (bStalled)
		return 0;

	// Nothing more to do for heartbeats
	if (stalledElement.kind == E_HEARTBEAT)
		return 0;

	if (outputQueue -> isFull()) {
		bStalled = true;
		return 0;
	}

	outputElement.kind         = stalledElement.kind;
	outputElement.timestamp    = stalledElement.timestamp;
	outputElement.tuple        = stalledElement.tuple;

	outputQueue -> enqueue (outputElement);
	//HR implementation by Lory Al Moakar
	// increment the number of tuples outputted so far
	n_tuples_outputted +=  1;
	// end of part 6 of HR implementation by LAM
	lastOutputTs = outputElement.timestamp;

	return 0;
}
//end of ArmaDILoS
