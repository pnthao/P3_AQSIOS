/**
 * @file         project.cc
 * @date         May 30, 2004
 * @brief        The projection operator
 */

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _PROJECT_
#include "execution/operators/project.h"
#endif
//HR implementation by Lory Al Moakar
#ifndef _COMPAREOPS_
#include "execution/operators/compareOPS.h"
#endif

#include <queue>
#include <math.h>
#include <stdlib.h>
#include <stdio.h>
//end of part 0 of HR implementation by LAM

using namespace Execution;
using namespace std;

#define LOCK_OUTPUT_TUPLE(t) (outStore -> addRef ((t)))
#define UNLOCK_INPUT_TUPLE(t) (inStore -> decrRef ((t)))

Project::Project(unsigned int id, ostream &_LOG)
	: LOG (_LOG)
{
	this -> id               = id;
	this -> inputQueue       = 0;
	this -> outputQueue      = 0;
	this -> evalContext      = 0;
	this -> projEval         = 0;
	this -> outStore         = 0;
	this -> inStore          = 0;
	this -> outSynopsis      = 0;
	this -> lastInputTs      = 0;
	this -> lastOutputTs     = 0;

	//HR implementation by Lory Al Moakar
	//initialize operator type and statistics 
	
	this -> operator_type = PROJECT;
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
	
	//ctrl-basedload shedding
#ifdef _CTRL_LOAD_MANAGE_	
	ctrl_out_tuple_count = 0;
	ctrl_num_of_queuing_tuples = 0;	
#endif	
	//end of load manager, by Thao Pham

}

Project::~Project() {
	if (evalContext)
		delete evalContext;
	if (projEval)
		delete projEval;
}

int Project::setInputQueue (Queue *inputQueue) 
{
	ASSERT (inputQueue);
	
	this -> inputQueue = inputQueue;
	return 0;
}

int Project::setOutputQueue (Queue *outputQueue) 
{
	ASSERT (outputQueue);
	
	this -> outputQueue = outputQueue;
	return 0;
}

int Project::setProjEvaluator (AEval *projEval)
{
	ASSERT (projEval);
	
	this -> projEval = projEval;
	return 0;
}

int Project::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);
	
	this -> evalContext = evalContext;
	return 0;
}

int Project::setOutStore (StorageAlloc *store)
{
	ASSERT (store);
	
	this -> outStore = store;
	return 0;
}

int Project::setInStore (StorageAlloc *store)
{
	ASSERT (store);
	
	this -> inStore = store;
	return 0;
}

int Project::setOutSynopsis (LineageSynopsis *outSynopsis)
{
	this -> outSynopsis = outSynopsis;
	return 0;
}

int Project::run (TimeSlice timeSlice) 
{
	unsigned int numElements;
	Element      inputElement;
	Tuple        outputTuple;
	Element      outputElement;
	int rc;

#ifdef _MONITOR_
	startTimer ();
#endif

/*#ifdef _CTRL_LOAD_MANAGE_
	ctrl_out_tuple_count = 0;
	//a project does not discard any tuple;
#endif*/

 	//HR implementation by Lory Al Moakar
	//get current time
	Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();

	//end of part 2 of HR implementation by LAM	
	
	// Number of elements to process
	numElements = timeSlice;
	unsigned int e = 0;
	
	for ( ; e < numElements ; e++) {
		
		// We are blocked @ output queue
		if (outputQueue -> isFull())
			break;
		
		// Get the next input element
		if (!inputQueue -> dequeue(inputElement)) {
		   break;		
		}

		//HR implementation by Lory Al Moakar
		// find out the num_tuples_processed so far
		num_tuples_processed +=  1;
		// end of part 3 of HR implementation by LAM
		
		// Timestamp of last input tuple: used in heartbeat generation
		lastInputTs = inputElement.timestamp;

		// Plus tuple
		if (inputElement.kind == E_PLUS) {
			
#ifdef _CTRL_LOAD_MANAGE_
			ctrl_num_of_queuing_tuples -=1;

#endif //_CTRL_LOAD_MANAGE	
			// Allocate space for output tuple
			if ((rc = outStore -> newTuple (outputTuple)) != 0) {				
				return rc;
			}
			
			// Produce the output tuple
			evalContext -> bind (outputTuple, OUTPUT_ROLE);
			evalContext -> bind (inputElement.tuple, INPUT_ROLE);
			projEval -> eval();
			
			// Produce the output element
			outputElement.kind = E_PLUS;
			outputElement.tuple = outputTuple;
			outputElement.timestamp = inputElement.timestamp;		
			
			// enqueue the output element
			outputQueue -> enqueue (outputElement);

#ifdef _CTRL_LOAD_MANAGE_

			for(int i=0;i<numOutputs;i++)
			{
				this->outputs[i]->ctrl_num_of_queuing_tuples +=1;
			}

#endif //_CTRL_LOAD_MANAGE	

			//HR implementation by Lory Al Moakar
			// increment the number of tuples outputted so far
			n_tuples_outputted +=  1;
			// end of part 5 of HR implementation by LAM

			lastOutputTs = outputElement.timestamp;
			
			if (outSynopsis) {
				rc = outSynopsis -> insertTuple (outputTuple, &inputElement.tuple);
				if (rc != 0) return rc;
				
				// Lock tuple to ensure memory manager does not deallocate
				// its space
				LOCK_OUTPUT_TUPLE(outputTuple);
			}
			
			// Discard tuple of input element
			UNLOCK_INPUT_TUPLE(inputElement.tuple);
		}
		
		else if (inputElement.kind == E_MINUS) {
			ASSERT (outSynopsis);
			
#ifdef _CTRL_LOAD_MANAGE_
			ctrl_num_of_queuing_tuples -=1;

#endif //_CTRL_LOAD_MANAGE	
			
			// Get the tuple that we produced for the corresponding PLUS tuple
			rc = outSynopsis -> getTuple (&inputElement.tuple,
										  outputTuple);
			if (rc != 0) return rc;
			
			outputElement.kind = E_MINUS;
			outputElement.tuple = outputTuple;
			outputElement.timestamp = inputElement.timestamp;
			
			outputQueue -> enqueue (outputElement);
			
#ifdef _CTRL_LOAD_MANAGE_
			for(int i=0;i<numOutputs;i++)
			{
				this->outputs[i]->ctrl_num_of_queuing_tuples +=1;
			}
	
#endif //_CTRL_LOAD_MANAGE	
			//HR implementation by Lory Al Moakar
			// increment the number of tuples outputted so far
			n_tuples_outputted +=  1;
			// end of part 6 of HR implementation by LAM
		
			lastOutputTs = outputElement.timestamp;
			
			// Delete the tuple from the synopsis
			rc = outSynopsis -> deleteTuple (outputTuple);
			if (rc != 0) return rc;
			
			// Discard tuple of input element
			UNLOCK_INPUT_TUPLE(inputElement.tuple);
		}
		
		// heartbeats: do nothing


	}
	
	// Generate heartbeat if necessary (and possible without blocking) 
	if (!outputQueue -> isFull() && (lastOutputTs < lastInputTs)) {
		outputQueue -> enqueue (Element::Heartbeat(lastInputTs));
		lastOutputTs = lastInputTs;
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 6 of HR implementation by LAM
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
	//end of part 4 of HR implementation by LAM
	
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
bool Project::calculateLocalStats() 
{
  // no tuples were processed in the last round
  // --> no need to adjust costs and selectivity
  if ( num_tuples_processed == 0){
    return false;
  }
  //calculate local selectivity & cost
  if (!firstRefresh) { 
   
    local_selectivity = (1-ALPHA) * local_selectivity +
      ALPHA * (double) n_tuples_outputted /(double) num_tuples_processed;
    
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
      //printf("OUTLIER current %f  new avg %f old avg %f \n", current_avg, local_cost_per_tuple,
      //     old_local_cost);
      local_cost_per_tuple  = old_local_cost;
      
    }
    
    //printf ( "PROJECT OPERATOR %d costs %llu  n_tuples %d smoothed %f \n",
    //     operator_id, local_cost, num_tuples_processed,local_cost_per_tuple);

  }
  else {
   
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

void Project::refreshPriority()
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
    std::priority_queue <Operator*,std::vector<Operator*>, CompareOPS> mypq;

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
      else if ( !(top_queue->operator_type == BIN_JOIN ||
		  top_queue->operator_type == BIN_STR_JOIN )){
	new_mean_cost += top_queue->mean_cost*local_selectivity;
	new_mean_selectivity +=  local_selectivity * 
	  top_queue->mean_selectivity;
      }	
      //we do not get here unless this operator is a window operator
      // since only window operators output to joins
      else {
	//my output is a join 
	int j = 0; 
	if ( top_queue->inputs[0]->operator_id == operator_id)
	  j = 1;
	double total_mean_costs = 0;
	for ( int k = 0; k < top_queue->numOutputs; k++)		
	  total_mean_costs+= top_queue->outputs[k]-> mean_cost;
	//check sharaf's TODS 2008 paper for formula
	new_mean_cost += local_selectivity * top_queue->local_cost_per_tuple + 
	  ( local_selectivity * top_queue->local_selectivity * 
	    top_queue->inputs[j]->local_selectivity *
	    top_queue->inputs[j]->n_tuples_win *
	    total_mean_costs);
	
	new_mean_selectivity += local_selectivity * top_queue->mean_selectivity
	  * top_queue->inputs[j]->local_selectivity
	  * top_queue->inputs[j]->n_tuples_win;
	
      } //this operator is a join
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
    if ( outputs[0]->operator_type == OUTPUT ||
         outputs[0]->operator_type == SINK ||
	 outputs[0]->firstRefresh == true){
         mean_cost = local_cost_per_tuple;
	 mean_selectivity = local_selectivity; 
	 }

    // is its output a join ?
    else if ( !(outputs[0]->operator_type == BIN_JOIN ||
		outputs[0]->operator_type == BIN_STR_JOIN ))       
      {
	mean_selectivity = local_selectivity * 
	  outputs[0]->mean_selectivity;
	
	mean_cost = outputs[0]->mean_cost * 
	  local_selectivity + mean_cost; 
	
      }	
    //its output is  a join
    else {
      //we do not get here unless this operator is a window operator
      // since only window operators output to joins
      //my output is a join 
      int j = 0; 
      if ( outputs[0]->inputs[0]->operator_id ==operator_id)
	j = 1;
      double total_mean_costs = 0;
      for ( int k = 0; k < outputs[0]->numOutputs; k++)		
	total_mean_costs+=outputs[0]->outputs[k]-> mean_cost;
      //check sharaf's TODS 2008 paper for formula
      mean_cost = local_cost_per_tuple + 
	local_selectivity * outputs[0]->local_cost_per_tuple  + 
	( local_selectivity * outputs[0]->local_selectivity *
	  outputs[0]->inputs[j]->local_selectivity *
	  outputs[0]->inputs[j]->n_tuples_win *
	  total_mean_costs);
				    
      mean_selectivity = local_selectivity *outputs[0]->mean_selectivity
	* outputs[0]->inputs[j]->local_selectivity
	* outputs[0]->inputs[j]->n_tuples_win;
    } //end of else
  }//end of if not shared
  
  //calculate priority
  priority = mean_selectivity / mean_cost;

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
int Project::readyToExecute() {
  if ( outputQueue->isFull() ) 
    return 0;
  return inputQueue->size();
}

// end of part 1 of HR with ready by LAM

//shedder embedded, by Thao Pham
int Project::run_with_shedder(TimeSlice timeSlice)
{
	unsigned int numElements;
		Element      inputElement;
		Tuple        outputTuple;
		Element      outputElement;
		int rc;

	#ifdef _MONITOR_
		startTimer ();
	#endif

	/*#ifdef _CTRL_LOAD_MANAGE_
		ctrl_out_tuple_count = 0;
		//a project does not discard any tuple;
	#endif*/

	 	//HR implementation by Lory Al Moakar
		//get current time
		Monitor::Timer mytime;
		unsigned long long int timeBeforeLoop = mytime.getCPUTime();

		//end of part 2 of HR implementation by LAM

		// Number of elements to process
		numElements = timeSlice;
		unsigned int e = 0;

		for ( ; e < numElements ; e++) {


			// We are blocked @ output queue
			if (outputQueue -> isFull())
				break;

			// Get the next input element
			if (!inputQueue -> dequeue(inputElement)) {
			   break;
			}

			// Ignore heartbeats
			if (inputElement.kind == E_HEARTBEAT)
				continue;

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

				// Timestamp of last input tuple: used in heartbeat generation
				lastInputTs = inputElement.timestamp;

				// Plus tuple
				if (inputElement.kind == E_PLUS) {

		#ifdef _CTRL_LOAD_MANAGE_
					ctrl_num_of_queuing_tuples -=1;

		#endif //_CTRL_LOAD_MANAGE
					// Allocate space for output tuple
					if ((rc = outStore -> newTuple (outputTuple)) != 0) {
						return rc;
					}

					// Produce the output tuple
					evalContext -> bind (outputTuple, OUTPUT_ROLE);
					evalContext -> bind (inputElement.tuple, INPUT_ROLE);
					projEval -> eval();

					// Produce the output element
					outputElement.kind = E_PLUS;
					outputElement.tuple = outputTuple;
					outputElement.timestamp = inputElement.timestamp;

					// enqueue the output element
					outputQueue -> enqueue (outputElement);

		#ifdef _CTRL_LOAD_MANAGE_

					for(int i=0;i<numOutputs;i++)
					{
						this->outputs[i]->ctrl_num_of_queuing_tuples +=1;
					}

		#endif //_CTRL_LOAD_MANAGE

					//HR implementation by Lory Al Moakar
					// increment the number of tuples outputted so far
					n_tuples_outputted +=  1;
					// end of part 5 of HR implementation by LAM

					lastOutputTs = outputElement.timestamp;

					if (outSynopsis) {
						rc = outSynopsis -> insertTuple (outputTuple, &inputElement.tuple);
						if (rc != 0) return rc;

						// Lock tuple to ensure memory manager does not deallocate
						// its space
						LOCK_OUTPUT_TUPLE(outputTuple);
					}

					// Discard tuple of input element
					UNLOCK_INPUT_TUPLE(inputElement.tuple);
				}

				else if (inputElement.kind == E_MINUS) {
					ASSERT (outSynopsis);

		#ifdef _CTRL_LOAD_MANAGE_
					ctrl_num_of_queuing_tuples -=1;

		#endif //_CTRL_LOAD_MANAGE

					// Get the tuple that we produced for the corresponding PLUS tuple
					rc = outSynopsis -> getTuple (&inputElement.tuple,
												  outputTuple);
					if (rc != 0) return rc;

					outputElement.kind = E_MINUS;
					outputElement.tuple = outputTuple;
					outputElement.timestamp = inputElement.timestamp;

					outputQueue -> enqueue (outputElement);

		#ifdef _CTRL_LOAD_MANAGE_
					for(int i=0;i<numOutputs;i++)
					{
						this->outputs[i]->ctrl_num_of_queuing_tuples +=1;
					}

		#endif //_CTRL_LOAD_MANAGE
					//HR implementation by Lory Al Moakar
					// increment the number of tuples outputted so far
					n_tuples_outputted +=  1;
					// end of part 6 of HR implementation by LAM

					lastOutputTs = outputElement.timestamp;

					// Delete the tuple from the synopsis
					rc = outSynopsis -> deleteTuple (outputTuple);
					if (rc != 0) return rc;

					// Discard tuple of input element
					UNLOCK_INPUT_TUPLE(inputElement.tuple);
				}
				// heartbeats: do nothing
			}
			else
			{
				UNLOCK_INPUT_TUPLE(inputElement.tuple);
			}


		}

		// Generate heartbeat if necessary (and possible without blocking)
		if (!outputQueue -> isFull() && (lastOutputTs < lastInputTs)) {
			outputQueue -> enqueue (Element::Heartbeat(lastInputTs));
			lastOutputTs = lastInputTs;
			//HR implementation by Lory Al Moakar
			// increment the number of tuples outputted so far
			n_tuples_outputted +=  1;
			// end of part 6 of HR implementation by LAM
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
		//end of part 4 of HR implementation by LAM


		return 0;
}
//end of embedded shedder
void Project::deactivate(){

	status = INACTIVE;
	//clear the input queue
	Element e;
	while (!inputQueue->isEmpty()) {
			inputQueue->dequeue(e);
		   UNLOCK_INPUT_TUPLE(e.tuple);

	}
	//clear the outsynopsis
	if(outSynopsis)
		outSynopsis->clearSyn(outStore);
}
