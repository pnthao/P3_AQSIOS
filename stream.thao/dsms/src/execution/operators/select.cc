/**
 * @file       select.cc
 * @date       May 30, 2004
 * @brief      Implementation of the select operator
 */

#ifndef _SELECT_
#include "execution/operators/select.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
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

#define UNLOCK_INPUT_TUPLE(t) (inStore -> decrRef ((t)))

Select::Select (unsigned int _id, ostream &_LOG)
	: LOG (_LOG)
{
	id           = _id;
	inputQueue   = 0;
	outputQueue  = 0;
	evalContext  = 0;
	predicate    = 0;
	lastInputTs  = 0;
	lastOutputTs = 0;
	inStore      = 0;
	//HR implementation by Lory Al Moakar
	//initialize operator type and statistics 
	
	this -> operator_type = SELECT;
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
	
	//ctrl-based load shedding
#ifdef _CTRL_LOAD_MANAGE_	
	ctrl_out_tuple_count =0;
	ctrl_num_of_queuing_tuples = 0;	
	simulated_cost = 1.5;
	cost_effective_time = 0;
	//f_cost_simulation = fopen("/home/thao/workspace/stream.test.thao/load_managing/cost_simulation", "r");
#endif	
	//end of load manager, by Thao Pham
}

Select::~Select() {
	
	if (evalContext)
		delete evalContext;
	
	if (predicate)
		delete predicate;
	
}

int Select::setInputQueue (Queue *inputQueue) 
{
	ASSERT (inputQueue);
	
	this -> inputQueue = inputQueue;
	return 0;
}

int Select::setOutputQueue (Queue *outputQueue) 
{
	ASSERT (outputQueue);
	
	this -> outputQueue = outputQueue;
	return 0;
}

int Select::setInStore (StorageAlloc *store)
{
	ASSERT (store);

	this -> inStore = store;
	return 0;
}

int Select::setPredicate (BEval *predicate) 
{
	ASSERT (predicate);
	
	this -> predicate = predicate;
	return 0;
}

int Select::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);
	
	this -> evalContext = evalContext;
	return 0;
}

int Select::run (TimeSlice timeSlice) 
{	
	unsigned int   numElements;
	Element        inputElement;

#ifdef _MONITOR_
	startTimer ();
#endif   
/*#ifdef _CTRL_LOAD_MANAGE_
	ctrl_out_tuple_count = 0;
#endif
*/
 	//HR implementation by Lory Al Moakar
	//get current time
	Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();
	int n_before = num_tuples_processed;
	//end of part 2 of HR implementation by LAM
	
	// Number of input elements to process
	numElements = timeSlice;
	unsigned int e = 0;
	for ( ; e < numElements ; e++) {
			
		// We are blocked @ output queue
		if (outputQueue -> isFull())
			break;
		
		
		// No more tuples to process
		if (!inputQueue -> dequeue (inputElement)){
		  break;
		}
		
		//sleep for the specified delay time
		//get the current time
		
/*		unsigned long long int c = this->getDelayTime()*1000; //c is expected to be in ns and getDelayTime returns time in microsec
		Monitor::Timer sleepTimer;
		unsigned long long int bTime = sleepTimer.getTime(); 
		while (sleepTimer.getTime()-bTime < c); 
*/		
//		unsigned long long int aTime = sleepTimer.getTime();
//		printf("%lld\n",aTime - bTime); 
		//HR implementation by Lory Al Moakar
		// find out the num_tuples_processed so far
		num_tuples_processed += 1;
		// end of part 3 of HR implementation by LAM

		// Timestamp of last input tuple: used in heartbeat generation
		lastInputTs = inputElement.timestamp;
		
		// Heartbeat: no filtering to be done
		if (inputElement.kind == E_HEARTBEAT)
			continue;
		
#ifdef _CTRL_LOAD_MANAGE_
		ctrl_num_of_queuing_tuples -=1;
#endif //_CTRL_LOAD_MANAGE	

		evalContext -> bind (inputElement.tuple, INPUT_CONTEXT);
		
		// The tuple satisfies the predicate: enqueue it to the output,
		// remember the timestamp (used in heartbeat generation)		
		if (predicate -> eval()) {			
			outputQueue -> enqueue (inputElement);
			
			//HR implementation by Lory Al Moakar
			// increment the number of tuples outputted so far
			n_tuples_outputted +=  1;
			// end of part 5 of HR implementation by LAM
			
			lastOutputTs = inputElement.timestamp;
			
#ifdef _CTRL_LOAD_MANAGE_
		for(int i=0;i<numOutputs;i++)
		{
			this->outputs[i]->ctrl_num_of_queuing_tuples +=1;
		}

#endif //_CTRL_LOAD_MANAGE	
		}
		
		// Tuple fails to satisfy the predicate: discard the tuple in the
		// input element.
		else {
			UNLOCK_INPUT_TUPLE (inputElement.tuple);
			
#ifdef _CTRL_LOAD_MANAGE_			
			ctrl_out_tuple_count ++; //here ctrl_out_tuple_count is the number of tuples leaving the system
			
#endif //_CTRL_LOAD_MANAGE_	
		}
		
	}
	
	//testing
	//printf("%d\n", e);
	
	// Heartbeat generation: Assert to the operator above that we won't
	// produce any element with timestamp < lastInputTs
	if (!outputQueue -> isFull() && (lastInputTs > lastOutputTs)) {
		outputQueue -> enqueue (Element::Heartbeat(lastInputTs));
		lastOutputTs = lastInputTs;
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
	if ( num_tuples_processed > n_before )//e > 1 ) 
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



bool Select::calculateLocalStats() 
{
  // no tuples were processed in the last round
  // --> no need to adjust costs and selectivity
  if ( num_tuples_processed == 0) {
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
    
    if ( (current_avg - avg_k) > OULIER_FACTOR*stdev) {
      //      printf("SELECT OUTLIER current %f  new avg %f old avg %f \n", current_avg, local_cost_per_tuple,
      //     old_local_cost);
      local_cost_per_tuple  = old_local_cost;
      
    }
    
    //printf ( "SELECTION OPERATOR %d costs %llu  n_tuples %d smoothed %f \n",
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

void Select::refreshPriority()
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
		  top_queue->operator_type == BIN_STR_JOIN)){
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
    else if (!( outputs[0]->operator_type == BIN_JOIN ||
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
  
  //printf("select cost: %f\n", local_cost_per_tuple);	
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
int Select::readyToExecute() {
  if ( outputQueue->isFull() ) 
    return 0;
  return inputQueue->size();
}

// end of part 1 of HR with ready by LAM

//by Thao Pham, to simulate the cost variation
int Select:: getDelayTime()
{
	Monitor::Timer timer;
	//unsigned long long int cur_time = timer.getTime() - system_start_time;
	unsigned long long int cur_time = timer.getTime() - system_start_time;
	if(ctrl_last_ts ==0)
		ctrl_last_ts = 0;
		
	unsigned long long int ctrl_period = cur_time - ctrl_last_ts;
	ctrl_last_ts = cur_time;
	
	
	unsigned long long int cost_length = 500000; //a cost is stable for 500ms
	//unsigned long long int rate_length = 20000000; //a rate is stable for 20s
	if(cost_effective_time == 0){
		cost_effective_time += cost_length*1000;
		int c;
		fscanf(f_cost_simulation, "%d",&c);
		simulated_cost = c;
		//printf("%d\n", ctrl_current_rate);
	}
	if(cur_time < cost_effective_time)
	{
		return simulated_cost;
		
	}
	else
	{
		if(!feof(f_cost_simulation))
		{	
			int c;
			if(fscanf(f_cost_simulation, "%d",&c)!=EOF){
				simulated_cost = c;
				
				cost_effective_time +=cost_length*1000;
			}
			//printf("ccc%f\n", num_of_incoming_tuples);
		}
		return simulated_cost;		 
	}
}
//end of cost simulation by Thao Pham

//embedded shedder, by Thao Pham
int Select::run_with_shedder(TimeSlice timeSlice)
{
	unsigned int   numElements;
		Element        inputElement;

	#ifdef _MONITOR_
		startTimer ();
	#endif
	/*#ifdef _CTRL_LOAD_MANAGE_
		ctrl_out_tuple_count = 0;
	#endif
	*/
	 	//HR implementation by Lory Al Moakar
		//get current time
		Monitor::Timer mytime;
		unsigned long long int timeBeforeLoop = mytime.getCPUTime();
		int n_before = num_tuples_processed;
		//end of part 2 of HR implementation by LAM

		// Number of input elements to process
		numElements = timeSlice;
		unsigned int e = 0;
		for ( ; e < numElements ; e++) {

			// We are blocked @ output queue
			if (outputQueue -> isFull())
				break;


			// No more tuples to process
			if (!inputQueue -> dequeue (inputElement)){
			  break;
			}

			// Heartbeat: no filtering to be done
			if (inputElement.kind == E_HEARTBEAT)
				continue;

			//drop or keep
			bool drop = 0;
			unsigned int r = rand()%100+1; //random number from 0-100

			if(r <= this->drop_percent){
				drop = 1;
			}
			//printf("drop: %d\n", drop_percent);
			if(!drop){
				//sleep for the specified delay time
				//get the current time

		/*		unsigned long long int c = this->getDelayTime()*1000; //c is expected to be in ns and getDelayTime returns time in microsec
				Monitor::Timer sleepTimer;
				unsigned long long int bTime = sleepTimer.getTime();
				while (sleepTimer.getTime()-bTime < c);
		*/
		//		unsigned long long int aTime = sleepTimer.getTime();
		//		printf("%lld\n",aTime - bTime);
				//HR implementation by Lory Al Moakar
				// find out the num_tuples_processed so far
				num_tuples_processed += 1;
				// end of part 3 of HR implementation by LAM

				// Timestamp of last input tuple: used in heartbeat generation
				lastInputTs = inputElement.timestamp;


		#ifdef _CTRL_LOAD_MANAGE_
				ctrl_num_of_queuing_tuples -=1;
		#endif //_CTRL_LOAD_MANAGE

				evalContext -> bind (inputElement.tuple, INPUT_CONTEXT);

				// The tuple satisfies the predicate: enqueue it to the output,
				// remember the timestamp (used in heartbeat generation)
				if (predicate -> eval()) {
					outputQueue -> enqueue (inputElement);

					//HR implementation by Lory Al Moakar
					// increment the number of tuples outputted so far
					n_tuples_outputted +=  1;
					// end of part 5 of HR implementation by LAM

					lastOutputTs = inputElement.timestamp;

		#ifdef _CTRL_LOAD_MANAGE_
				for(int i=0;i<numOutputs;i++)
				{
					this->outputs[i]->ctrl_num_of_queuing_tuples +=1;
				}

		#endif //_CTRL_LOAD_MANAGE
				}

				// Tuple fails to satisfy the predicate: discard the tuple in the
				// input element.
				else {
					UNLOCK_INPUT_TUPLE (inputElement.tuple);

		#ifdef _CTRL_LOAD_MANAGE_
					ctrl_out_tuple_count ++; //here ctrl_out_tuple_count is the number of tuples leaving the system

		#endif //_CTRL_LOAD_MANAGE_
				}
			}
			else{//drop
				UNLOCK_INPUT_TUPLE (inputElement.tuple);
				//printf("hehe");

			}

		}

		//testing
		//printf("%d\n", e);

		// Heartbeat generation: Assert to the operator above that we won't
		// produce any element with timestamp < lastInputTs
		if (!outputQueue -> isFull() && (lastInputTs > lastOutputTs)) {
			outputQueue -> enqueue (Element::Heartbeat(lastInputTs));
			lastOutputTs = lastInputTs;
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
		if ( num_tuples_processed > n_before )//e > 1 )
		  local_cost += timeAfterLoop - timeBeforeLoop;

		//end of part 4 of HR implementation by LAM

		return 0;
}
//end of embedded shedder, by Thao Pham

//ArmaDILoS, by Thao Pham
void Select::deactivate(){
	status = INACTIVE;
	Element e;
	while(!inputQueue->isEmpty()){
		inputQueue->dequeue(e);
		UNLOCK_INPUT_TUPLE(e.tuple);
	}
	if(outputQueue->isEmpty()){
		for(int i=0;i<numOutputs;i++){
			outputs[i]->deactivate();
		}
	}
}
//end of ArmaDILoS, by Thao Pham
