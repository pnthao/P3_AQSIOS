#ifndef _SINK_
#include "execution/operators/sink.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

using namespace Execution;

#define UNLOCK_INPUT_TUPLE(t) (inStore -> decrRef ((t)))

Sink::Sink (unsigned int id, ostream &_LOG)
	: LOG (_LOG)
{
	this -> id = id;
	this -> inputQueue = 0;
	this -> inStore = 0;
	//HR implementation by Lory Al Moakar
	//initialize operator type and statistics 
	
	this -> operator_type = SINK;
	this -> mean_cost = 1;
	this -> mean_selectivity = 1;
	this -> local_selectivity =1;
	this -> n_tuples_outputted = 0;
	this -> local_cost = 1;
	// we want sink operators to have a high priority 
	// because we want to drain tuples fast from the 
	// system
	this -> priority = 10; 
	this -> num_tuples_processed = 0;

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

Sink::~Sink () {}

int Sink::setInputQueue (Queue *inputQueue)
{
	ASSERT (inputQueue);

	this -> inputQueue = inputQueue;
	return 0;
}

int Sink::setInStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> inStore = store;
	return 0;
}

int Sink::run (TimeSlice timeSlice)
{
	Element inputElement;
	unsigned int numElements;	
	
#ifdef _MONITOR_
	startTimer ();
#endif
	
	numElements = timeSlice;
	
	for (unsigned int e = 0 ; e < numElements ; e++) {
		// Get the next element
	        if (!inputQueue -> dequeue (inputElement)){
		  break;
		}
		
		// Ignore heartbeats
		if (inputElement.kind != E_HEARTBEAT)
			UNLOCK_INPUT_TUPLE (inputElement.tuple);

		//Lottery scheduling by Lory Al Moakar
		// find out the num_tuples_processed so far
		num_tuples_processed +=  1;
		// end of part 1 of Lottery scheduling by LAM
	}

#ifdef _MONITOR_
	stopTimer ();
#endif
	//deactivate itself it there is no more incoming tuples to expect
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
bool Sink::calculateLocalStats(){ return true; }

/** this method is used in order to calculate the priority
 * of the operator. This is used in order to assign the priorities
 * used by the scheduler to schedule operators.
 */

void Sink::refreshPriority(){}
// end of part 1 of HR implementation by LAM
//HR with ready by Lory Al Moakar 
/** this method returns how ready this operator is to 
 * execute. If it returns a positive number --> ready
 * If it returns a zero or negative number --> not ready
 */
int Sink::readyToExecute() {
  return inputQueue->size();
}

// end of part 1 of HR with ready by LAM
int Sink::run_with_shedder (TimeSlice timeSlice)
{
	return 0;
}
void Sink::deactivate(){

	status = INACTIVE;
	Element e;
	while(!inputQueue->isEmpty()){
		inputQueue->dequeue(e);
		UNLOCK_INPUT_TUPLE(e.tuple);
	}
}
