/**
 * @file           bin_str_join.cc
 * @date           May 30, 2004
 * @brief          Binary stream join
 */

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _BIN_STR_JOIN_
#include "execution/operators/bin_str_join.h"
#endif
//HR implementation by Lory Al Moakar
#ifndef _COMPAREOPS_
#include "execution/operators/compareOPS.h"
#endif

#include <queue>
#include <math.h>
//end of part 0 of HR implementation by LAM

#define MIN(a,b) (((a) < (b))? (a) : (b))

#define LOCK_INNER_TUPLE(t) (innerInStore -> addRef ((t)))
#define LOCK_OUTPUT_TUPLE(t) (outStore -> addRef ((t)))
#define UNLOCK_OUTPUT_TUPLE(t) (outStore -> decrRef ((t)))
#define UNLOCK_INNER_TUPLE(t) (innerInStore -> decrRef ((t)))
#define UNLOCK_OUTER_TUPLE(t) (outerInStore -> decrRef ((t)))
#define LOCK_OUTER_TUPLE(t) (outerInStore -> addRef ((t)))


using namespace Execution;
using namespace std;
BinStreamJoin::BinStreamJoin(unsigned int id, std::ostream &_LOG)
	: LOG (_LOG)
{
	this -> id                      = id;
	this -> outerInputQueue         = 0;
	this -> innerInputQueue         = 0;
	this -> outputQueue             = 0;	
	this -> innerSynopsis           = 0;
	this -> scanId                  = 0;
	this -> outStore                = 0;
	this -> outerInStore            = 0;
	this -> innerInStore            = 0;
	this -> evalContext             = 0;
	this -> outputConstructor       = 0;	
	this -> lastOuterTs             = 0;
	this -> lastInnerTs             = 0;
	this -> lastOutputTs            = 0;
	
	this -> bStalled                = false;
	this -> innerScanWhenStalled    = 0;

	//HR implementation by Lory Al Moakar
	//initialize operator type and statistics 
	
	this -> operator_type = BIN_STR_JOIN;
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
#ifdef _CTRL_LOAD_MANAGE_
	ctrl_num_of_queuing_tuples = 0;
#endif //_CTRL_LOAD_MANAGE	
	//end of load manager, by Thao Pham
}

BinStreamJoin::~BinStreamJoin()
{
	if (evalContext)
		delete evalContext;
	if (outputConstructor)
		delete outputConstructor;	
}

int BinStreamJoin::setOuterInputQueue (Queue *outerQueue) 
{
	ASSERT (outerQueue);
	
	this -> outerInputQueue = outerQueue;
	return 0;
}

int BinStreamJoin::setInnerInputQueue (Queue *innerQueue)
{
	ASSERT (innerQueue);
	
	this -> innerInputQueue = innerQueue;
	return 0;
}

int BinStreamJoin::setOutputQueue (Queue *outputQueue)
{
	ASSERT (outputQueue);
	this -> outputQueue = outputQueue;
	return 0;
}

int BinStreamJoin::setSynopsis (RelationSynopsis *innerSynopsis)
{
	ASSERT (innerSynopsis);
	
	this -> innerSynopsis = innerSynopsis;
	return 0;
}

int BinStreamJoin::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);
	
	this -> evalContext = evalContext;
	return 0;
}

int BinStreamJoin::setScan (unsigned int scanId)
{	
	this -> scanId   = scanId;
	return 0;
}

int BinStreamJoin::setOutputStore (StorageAlloc *store)
{
	ASSERT (store);
	
	this -> outStore = store;
	return 0;
}

int BinStreamJoin::setOuterInputStore (StorageAlloc* store)
{
	ASSERT (store);

	this -> outerInStore = store;
	return 0;
}

int BinStreamJoin::setInnerInputStore (StorageAlloc* store)
{
	ASSERT (store);

	this -> innerInStore = store;
	return 0;
}

int BinStreamJoin::setOutputConstructor (AEval *outputConstructor)
{
	ASSERT (outputConstructor);
	
	this -> outputConstructor = outputConstructor;
	return 0;
}

int BinStreamJoin::run(TimeSlice timeSlice)
{
	int rc;
	unsigned int numElements;
	Timestamp    innerMinTs, outerMinTs;
	Element      outerPeekElement, innerPeekElement;	
	Element      outerElement, innerElement;


#ifdef _MONITOR_
	startTimer ();
#endif

 	//HR implementation by Lory Al Moakar
	//get current time
	Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();

	//end of part 2 of HR implementation by LAM

	// I have a stall 
	if (bStalled) {
		// Try clearing it ...
		if ((rc = clearStall()) != 0)
			return rc;
		
		// ... I failed 
		if (bStalled) {

#ifdef _MONITOR_
			stopTimer ();
			logOutTs (lastOutputTs);
#endif
			
			return 0;
		}
	}
	
	// Minimum timestamp possible on the next outer element
	outerMinTs = lastOuterTs;
	
	// Minimum timestamp possible on the next inner element
	innerMinTs = lastInnerTs;

	numElements = timeSlice;
		
	unsigned int e = 0 ;
	
	for ( ; (e < numElements) && !bStalled ; e++) {		
		
		// Peek to revise min timestamp estimate
		if (outerInputQueue -> peek (outerPeekElement))
			outerMinTs = outerPeekElement.timestamp;
		
		// Peek to revise min inner timestamp estimate
		if (innerInputQueue -> peek (innerPeekElement))
			innerMinTs = innerPeekElement.timestamp;
		
		if (outerMinTs < innerMinTs) {
			
			// If outer has an element, then I know that all future
			// elements of inner have a timestamp *strictly* greater than
			// the timestamp of this (next outer) element.  Note that
			// "strictly greater" is necessary for correct semantics						
			if (outerInputQueue -> dequeue(outerElement)) {
			        //HR implementation by Lory Al Moakar
			        // find out the num_tuples_processed so far
			        num_tuples_processed += 1;
				// end  of part 3 of HR implementation by LAM
				
				lastOuterTs = outerElement.timestamp;
				

				// Heartbeats require no processing
				if (outerElement.kind == E_HEARTBEAT)
					continue;

#ifdef _CTRL_LOAD_MANAGE_
					ctrl_num_of_queuing_tuples -=1;

#endif //_CTRL_LOAD_MANAGE	
				
				// We might stall inside processOuter, in which case
				// bStalled is set.  Note that the for loop terminates in
				// that case.
				if ((rc = processOuter (outerElement)) != 0)
					return rc;
				
				UNLOCK_OUTER_TUPLE (outerElement.tuple);

			}
			
			// If outer does not have an element, I cannot do any
			// processing ... 
			else {	
			  //printf("blocked join\n");
			  break;
			}
		}
		
		else {
			
			// If inner has an element, then I know that all future
			// elements of outer have a timestamp at least as much as that
			// of this (next inner) element.  
			if (innerInputQueue -> dequeue (innerElement)) {
				
			        //HR implementation by Lory Al Moakar
			        // find out the num_tuples_processed so far
			        num_tuples_processed += 1;
			        // end of part 3 of HR implementation by LAM
			       
			        lastInnerTs = innerElement.timestamp;			
				
				// Heartbeats require no processing
				if (innerElement.kind == E_HEARTBEAT)
					continue;
				
#ifdef _CTRL_LOAD_MANAGE_
					ctrl_num_of_queuing_tuples -=1;
#endif //_CTRL_LOAD_MANAGE	
				// processInner does not generate any output -- so we will
				// never stall here.
				if ((rc = processInner (innerElement)) != 0)
					return rc;
				
				UNLOCK_INNER_TUPLE (innerElement.tuple);
			}
			
			// Else I am stuck ...
			else {  
			  //printf("blocked join\n");
				break;
			}
		}

	}
	
	// Heartbeat generation
	if (!outputQueue -> isFull() && (lastOutputTs < innerMinTs) &&
		(lastOutputTs < outerMinTs)) {
		ASSERT (!bStalled);
		
		lastOutputTs = MIN (outerMinTs, innerMinTs);
		
		outputQueue -> enqueue (Element::Heartbeat (lastOutputTs));
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

	//end of part 4 of HR implementation by LAM	
		
	return 0;
}

int BinStreamJoin::processInner (Element innerElement)
{		
	int rc;
	
	if (innerElement.kind == E_PLUS) {

		LOCK_INNER_TUPLE (innerElement.tuple);
		
		rc = innerSynopsis -> insertTuple (innerElement.tuple);
		if (rc != 0) return rc;
				
	}
	else {
		
		ASSERT (innerElement.kind == E_MINUS);
		
		rc = innerSynopsis -> deleteTuple (innerElement.tuple);
		if (rc != 0) return rc;
		
		UNLOCK_INNER_TUPLE (innerElement.tuple);
	}
	
	return 0;
}

int BinStreamJoin::processOuter (Element outerElement)
{
	int rc;
	TupleIterator *innerScan;
	Tuple joinTuple;
	Tuple outputTuple;
	Element outputElement;

#ifdef _MONITOR_
	logInput ();
#endif
	
	ASSERT (!bStalled && !innerScanWhenStalled);
	
	// Recall: outer is a stream
	ASSERT (outerElement.kind == E_PLUS);
	
	evalContext -> bind (outerElement.tuple, OUTER_ROLE); 
	
	if ((rc = innerSynopsis -> getScan (scanId, innerScan)) != 0)
		return rc;
	
	while (!outputQueue -> isFull() && innerScan -> getNext (joinTuple)) {
		
		if ((rc = outStore -> newTuple (outputTuple)) != 0)
			return rc;
		
		evalContext -> bind (joinTuple, INNER_ROLE);
		evalContext -> bind (outputTuple, OUTPUT_ROLE);
		outputConstructor -> eval();
		
		outputElement.kind      = E_PLUS;
		outputElement.tuple     = outputTuple;
		outputElement.timestamp = outerElement.timestamp;
		
		outputQueue -> enqueue (outputElement);
		
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 5 of HR implementation by LAM
	  		
		lastOutputTs = outputElement.timestamp;

#ifdef _CTRL_LOAD_MANAGE_
		for(int i=0;i<numOutputs;i++)
		{
			this->outputs[i]->ctrl_num_of_queuing_tuples +=1;
		}

#endif //_CTRL_LOAD_MANAGE	

#ifdef _MONITOR_
		logJoin ();
#endif

	}
	
	// We stalled: It is possible that innerScan is also over, in which
	// case we are strictly not stalled, but we will discover that late
	// any way ...
	if (outputQueue -> isFull()) {
		bStalled = true;
		innerScanWhenStalled = innerScan;
		stallTuple = outerElement.tuple;
		
		LOCK_OUTER_TUPLE (stallTuple);
	}
	
	else {
		if ((rc = innerSynopsis -> releaseScan (scanId, innerScan)) != 0)
			return rc;
	}
	
	return 0;
}

int BinStreamJoin::clearStall ()
{
	int rc;
	TupleIterator *innerScan;
	Tuple joinTuple;
	Tuple outputTuple;
	Element outputElement;
	
	ASSERT (bStalled);
	ASSERT (innerScanWhenStalled);	
	
	innerScan = innerScanWhenStalled;	
	while (!outputQueue -> isFull () && innerScan -> getNext (joinTuple)) {
		
		if ((rc = outStore -> newTuple (outputTuple)) != 0)
			return rc;
		
		evalContext -> bind (joinTuple, INNER_ROLE);
		evalContext -> bind (outputTuple, OUTPUT_ROLE);
		outputConstructor -> eval();
		
		outputElement.kind      = E_PLUS;
		outputElement.tuple     = outputTuple;
		outputElement.timestamp = lastOuterTs;
		
		outputQueue -> enqueue (outputElement);

		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 5 of HR implementation by LAM

		lastOutputTs = lastOuterTs;

#ifdef _CTRL_LOAD_MANAGE_
		for(int i=0;i<numOutputs;i++)
		{
			this->outputs[i]->ctrl_num_of_queuing_tuples +=1;
		}

#endif //_CTRL_LOAD_MANAGE	

#ifdef _MONITOR_
		logJoin ();
#endif
		
	}
	
	// We cleared the stall
	if (!outputQueue -> isFull()) {
		bStalled = false;
		innerScanWhenStalled = 0;
		
		if ((rc = innerSynopsis -> releaseScan (scanId, innerScan)) != 0)
			return rc;
		
		UNLOCK_OUTER_TUPLE (stallTuple);
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
bool BinStreamJoin::calculateLocalStats() 
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
      //printf("OUTLIER current %f  new avg %f old avg %f \n", current_avg, local_cost_per_tuple,
      //     old_local_cost);
      local_cost_per_tuple  = old_local_cost;
      
    }
    
    // printf ( "BIN STR OPERATOR %d costs %llu  n_tuples %d smoothed %f \n",
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

void BinStreamJoin::refreshPriority()
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
	if ( top_queue->inputs[0]->operator_id ==operator_id)
	  j = 1;
	double total_mean_costs = 0;
	for ( int k = 0; k < top_queue->numOutputs; k++)		
	  total_mean_costs+= top_queue->outputs[k]-> mean_cost;
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
int BinStreamJoin::readyToExecute() {
  if ( outputQueue->isFull() ) 
    return 0;
  return min ( innerInputQueue->size(), outerInputQueue->size() );
}

// end of part 1 of HR with ready by LAM
int BinStreamJoin::run_with_shedder (TimeSlice timeSlice)
{
	return 0;
}
//ArmaDILoS, by Thao Pham
void BinStreamJoin::deactivate(){

	status = INACTIVE;

	bStalled = false;
	Element e;
	while(!innerInputQueue->isEmpty()){
		innerInputQueue->dequeue(e);
		UNLOCK_INNER_TUPLE(e.tuple);
	}
	while(!outerInputQueue->isEmpty()){
		outerInputQueue->dequeue(e);
		UNLOCK_OUTER_TUPLE(e.tuple);
	}
	Tuple t;
	if(innerSynopsis)
		((RelationSynopsis*)innerSynopsis)->clearSyn(innerInStore);

}
//end of ArmaDILos
