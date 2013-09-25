/**
 * @file       heavyweight_drop.cc
 * @date       Feb 21, 2009
 * @brief      Implementation of the drop operator
 * @author		Thao Pham
 */

#ifndef _HEAVYWEIGHT_DROP_
#include "execution/operators/heavyweight_drop.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#include <stdlib.h>

using namespace Execution;
using namespace std;

#define UNLOCK_INPUT_TUPLE(t) (inStore -> decrRef ((t)))
#define LOCK_OUTPUT_TUPLE(t) (outStore -> addRef ((t)))

HeavyWeightDrop::HeavyWeightDrop (unsigned int _id, ostream &_LOG)
	: LOG (_LOG)
{
	this->id           = _id;
	this->inputQueue   = 0;
	this->outputQueue  = 0;
	this->inStore      = 0;
	this->outStore     = 0;
	this-> copyEval    = 0;
	this->evalContext  = 0;
	this -> lastInputTs = 0;
	this -> lastOutputTs = 0;
	
	this-> dropPercent = 0;
	//this-> active 	   = false;
	dropCount    = 0;
	tupleCount   = 0;
	
	//HR implementation by Lory Al Moakar
	//initialize operator type and statistics 
	
	this -> operator_type = EXCEPT;
	this -> mean_cost = 1;
	this -> mean_selectivity = 1;
	this -> local_selectivity =1;
	this -> n_tuples_outputted = 0;
	this -> local_cost = 1;
	this -> priority = 1;
	this -> num_tuples_processed = 0;
	this -> firstRefresh = true;
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
	
#ifdef _CTRL_LOAD_MANAGE_
	ctrl_out_tuple_count = 0;
#endif
	
}
HeavyWeightDrop::~HeavyWeightDrop() 
{
	if(evalContext) 
		delete evalContext;
	if (copyEval) 
		delete copyEval;
}

int HeavyWeightDrop::setOutStore(StorageAlloc * store)
{
	ASSERT(store);
	this->outStore = store;
	return 0;
}
int HeavyWeightDrop::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);
	this -> evalContext = evalContext;
	return 0;
}

int HeavyWeightDrop::setCopyEval (AEval *eval)
{
	ASSERT (eval);
	
	this -> copyEval = eval;
	return 0;
}

int HeavyWeightDrop::run (TimeSlice timeSlice) 
{	
	int rc;
	unsigned int    numElements;
	Element         inputElement;
	Tuple        	outputTuple;
	Element      	outputElement;
	
#ifdef _MONITOR_
	startTimer ();
#endif   
	
	//local cost computation, by Thao Pham
	//get current time
	Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();
	int n_before = num_tuples_processed;
	//end of part 1 of local cost computation by Thao Pham
	
	// Number of input elements to process
	numElements = timeSlice;
	//srand(time(NULL));
	for (unsigned int e = 0 ; e < numElements ; e++) {
		
		// We are blocked @ output queue
		if (outputQueue -> isFull())
			break;
		
		// No more tuples to process
		if (!inputQueue -> dequeue (inputElement))
			break;
			
		//we expect that no minus tupes appears in the input of the drop
		
		ASSERT (inputElement.kind != E_MINUS);
		
		//local cost computation by Thao Pham
		// find out the num_tuples_processed so far
		num_tuples_processed += 1;
		// end of part 2 of local cost computation, by Thao Pham
		
		lastInputTs = inputElement.timestamp;
		
		// Heartbeat: ignore
		if (inputElement.kind == E_HEARTBEAT)
			continue;
		
		int drop = 0; //default decision is keep
		
		//in this case, need to drop to enforce the drop amount set, no choice 
		if((100-this->tupleCount+this->dropCount) <= this->dropPercent){
			drop = 1;
		}
		else{
			/*if we haven't drop enough in this cycle
			 * flip a coin with probability for drop of dropPercent%
			 * assume that the probability for a random number in the range 100 to be
			 * less than dropPercent is dropPercent%
			 */ 
			if(this->dropCount < this->dropPercent){
				
				unsigned int r = rand()%100+1; //random number from 0-100
				//printf("%d",r);
				if(r < this->dropPercent){
					drop = 1;
				}
			}
		}
		//if result is "keep"
		if(!drop){			
			if((rc=outStore-> newTuple(outputTuple))!=0)
				return rc;
			//copy input tuple to output tuple
			evalContext->bind(inputElement.tuple, INPUT_ROLE);
			evalContext->bind(outputTuple, OUTPUT_ROLE);
			copyEval->eval();
			
			// Generate the output element
			outputElement.timestamp = lastInputTs;
			outputElement.tuple = outputTuple;
			outputElement.kind = E_PLUS;
				
			//add to the output queue
			outputQueue -> enqueue (outputElement);
			
			//update last output timestamp, used to generate heartbeat
			lastOutputTs = lastInputTs;
			LOCK_OUTPUT_TUPLE(outputTuple);
			//decrease reference to input tuple
			UNLOCK_INPUT_TUPLE (inputElement.tuple);
		}
		// result is "drop": discard the tuple in the
		// input element.
		else 
		{
			UNLOCK_INPUT_TUPLE (inputElement.tuple);
			this->dropCount = this->dropCount+1; 
			
#ifdef _CTRL_LOAD_MANAGE_			
			this->ctrl_out_tuple_count++;
#endif
			
		}
		if(this->tupleCount == 99)
		{
			this->tupleCount=0;
			this->dropCount =0;
		}
		else
		{
			this->tupleCount = this->tupleCount+1;
		}
		
		/*//Lottery scheduling by Lory Al Moakar
		// find out the num_tuples_processed so far
		num_tuples_processed = e + 1;
		// end of part 1 of Lottery scheduling by LAM*/
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
	//HR implementation by Lory Al Moakar
	//get current time	
	unsigned long long int timeAfterLoop = mytime.getCPUTime();
	
	//calculate time elapsed during execution
	//add it to local cost
	if ( num_tuples_processed > n_before )//e > 1 ) 
	  local_cost += timeAfterLoop - timeBeforeLoop;

	//end of part 4 of HR implementation by LAM

	return 0;
}

int HeavyWeightDrop::run_with_shedder (TimeSlice timeSlice)
{
	return 0;
}
