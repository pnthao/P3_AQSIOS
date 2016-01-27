/**
 * @file   distinct.cc
 * @date   Aug. 26, 2004
 * @brief  Implements the distinct operator
 */

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _DISTINCT_
#include "execution/operators/distinct.h"
#endif
//HR implementation by Lory Al Moakar

#ifndef _COMPAREOPS_
#include "execution/operators/compareOPS.h"
#endif

#include <queue>
#include <math.h>
//end of part 0 of HR implementation by LAM

#define LOCK_OUTPUT_TUPLE(t) (outStore -> addRef ((t)))
#define UNLOCK_INPUT_TUPLE(t) (inStore -> decrRef ((t)))


using namespace Execution;

Distinct::Distinct (unsigned int id, std::ostream &_LOG)
	: LOG (_LOG)
{
	this -> id                  = id;
	this -> inputQueue          = 0;
	this -> outputQueue         = 0;
	this -> outputSynopsis      = 0;
	this -> outStore            = 0;
	this -> inStore             = 0;
	this -> evalContext         = 0;
	this -> plusEval            = 0;
	this -> initEval            = 0;	
	this -> minusEval           = 0;
	this -> emptyEval           = 0;	
	this -> lastInputTs         = 0;
	this -> lastOutputTs        = 0;

	//HR implementation by Lory Al Moakar
	//initialize operator type and statistics 
	
	this -> operator_type = DISTINCT;
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
}

Distinct::~Distinct ()
{
	if (evalContext)
		delete evalContext;
	if (plusEval)
		delete plusEval;
	if (initEval)
		delete initEval;
	if (minusEval)
		delete minusEval;
	if (emptyEval)
		delete emptyEval;
}

int Distinct::setInputQueue (Queue *inputQueue)
{
	ASSERT (inputQueue);
	
	this -> inputQueue = inputQueue;
	return 0;
}

int Distinct::setOutputQueue (Queue *outputQueue)
{
	ASSERT (outputQueue);
	
	this -> outputQueue = outputQueue;
	return 0;
}

int Distinct::setOutputSynopsis (RelationSynopsis *synopsis,
								 unsigned int scanId)
{
	ASSERT (synopsis);
	
	this -> outputSynopsis = synopsis;
	this -> outScanId = scanId;
	return 0;
}

int Distinct::setOutStore (StorageAlloc *store)
{
	ASSERT (store);
	
	this -> outStore = store;
	return 0;
}

int Distinct::setInStore (StorageAlloc *store)
{
	ASSERT (store);
	
	this -> inStore = store;
	return 0;
}

int Distinct::setEvalContext (EvalContext *evalContext)
{
	ASSERT (evalContext);
	
	this -> evalContext = evalContext;
	return 0;
}

// could be null
int Distinct::setPlusEvaluator (AEval *plusEval)
{	
	this -> plusEval = plusEval;
	return 0;
}

// could be null
int Distinct::setInitEvaluator (AEval *initEval)
{
	this -> initEval = initEval;
	return 0;
}

// could be null
int Distinct::setMinusEvaluator (AEval *minusEval)
{
	this -> minusEval = minusEval;
	return 0;
}

// could be null
int Distinct::setEmptyEvaluator (BEval *emptyEval)
{
	this -> emptyEval = emptyEval;
	return 0;
}

int Distinct::run (TimeSlice timeSlice)
{
	int rc;
	unsigned int numElements;
	Element      inputElement;

#ifdef _MONITOR_
	startTimer ();
#endif	

 	//HR implementation by Lory Al Moakar
	//get current time
	Monitor::Timer mytime;
	unsigned long long int timeBeforeLoop = mytime.getCPUTime();

	//end of part 2 of HR implementation by LAM						
	
	numElements = timeSlice;

	unsigned int e = 0;
	
	
	for ( ; e < numElements ; e++) {

		// No space in output queue -- we quit
		if (outputQueue -> isFull())
		  break;

		// Get the next input element
		if (!inputQueue -> dequeue (inputElement)) {
		  break;
		}
		
		//HR implementation by Lory Al Moakar
		// find out the num_tuples_processed so far
		num_tuples_processed += 1;
		// end of part 3 of HR implementation by LAM
		
		lastInputTs = inputElement.timestamp;

		// Heartbeats can be ignored
		if (inputElement.kind == E_HEARTBEAT)
			continue;
		
		if (inputElement.kind == E_PLUS) {
			rc = processPlus (inputElement);
			if (rc != 0) return rc;

		}
		
		else {
			ASSERT (inputElement.kind == E_MINUS);
			rc = processMinus (inputElement);
			if (rc != 0) return rc;
		}
		
		UNLOCK_INPUT_TUPLE (inputElement.tuple);


	}
	
	// process heartbeats
	if (!outputQueue -> isFull() && (lastOutputTs < lastInputTs)) {
		outputQueue -> enqueue (Element::Heartbeat (lastInputTs));
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
	if ( e > 0 )
	  local_cost += timeAfterLoop - timeBeforeLoop;

	//end of part 4 of HR implementation by LAM
	if(inputQueue->isEmpty()&&inputs[0]->status==INACTIVE)
		deactivate();
		 
	return 0;
}

int Distinct::processPlus (Element inputElement)
{
	int rc;

	// input tuple
	Tuple inTuple; 
	
	// The synopsis tuple for inputElement.tuple
	Tuple synTuple;
	
	// Scan iterator for retrieving synTuple from outputSynopsis
	TupleIterator *scan;

	// ...
	bool bSynTupleExists;

	// Output element
	Element plusElement;	
	
	inTuple = inputElement.tuple;
	
	evalContext -> bind (inTuple, INPUT_ROLE);

	// Get a scan that produces the syn tuple corresponding to the input
	// tuple
	if ((rc = outputSynopsis -> getScan (outScanId, scan)) != 0)
		return rc;
	
	bSynTupleExists = scan -> getNext (synTuple);

#ifdef _DM_
	Tuple dummy;
	ASSERT (!scan -> getNext (dummy));
#endif

	if ((rc = outputSynopsis -> releaseScan (outScanId, scan)) != 0)
		return rc;
	
	// We have seen an identical tuple before, so we should not output
	// this tuple.  But we update a counter so that we know how many
	// identical tuples we have seen.  This update is done in-place.  This
	// update needs to be done only if the input can produce minus tuples
	
	if (bSynTupleExists) {
		if (plusEval) {
			evalContext -> bind (synTuple, SYN_ROLE);
			plusEval -> eval();
		}
	}
	
	else {
		
		rc = outStore -> newTuple (synTuple);
		if (rc != 0) return rc;
		
		// syn tuple with count = 1
		evalContext -> bind (synTuple, SYN_ROLE);
		initEval -> eval ();
		
		// Insert the tuple to the output synopsis.
		rc = outputSynopsis -> insertTuple (synTuple);
		if (rc != 0) return rc;
		LOCK_OUTPUT_TUPLE (synTuple);
		
		// Send a PLUS element for this tuple.  btw, we know we are not
		// blocked because we checked.
		ASSERT (!outputQueue -> isFull());
		
		plusElement.kind = E_PLUS;
		plusElement.tuple = synTuple;
		plusElement.timestamp = inputElement.timestamp;
		
		outputQueue -> enqueue (plusElement);
		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 5 of HR implementation by LAM
	}
	
	return 0;
}

int Distinct::processMinus (Element inputElement)
{
	int rc;
	
	// Input tuple
	Tuple inTuple;
	
	// The syn tuple for all tuples identical to inputTuple
	Tuple synTuple;
	
	// Scan to get the count tuple
	TupleIterator *scan;
	
	// ...
	bool bSynTupleExists;

	Element minusElement;
	
	inTuple = inputElement.tuple;
	evalContext -> bind (inTuple, INPUT_ROLE);
	
	rc = outputSynopsis -> getScan (outScanId, scan);
	if (rc != 0) return rc;

	// Since we are seeing a MINUS tuple, we should have record of this
	// tuple.
	bSynTupleExists = scan -> getNext (synTuple);
#ifdef _DM_
	ASSERT (bSynTupleExists);
	Tuple dummy;
	ASSERT (!scan -> getNext (dummy));
#endif
	if ((rc = outputSynopsis -> releaseScan (outScanId, scan)) != 0)
		return rc;
	
	evalContext -> bind (synTuple, SYN_ROLE);
		
	// If count == 1, output a MINUS tuple
	if (emptyEval -> eval ()) {		
		minusElement.kind = E_MINUS;
		minusElement.tuple = synTuple;
		minusElement.timestamp = inputElement.timestamp;
		
		// We have ensured before coming here that the output queue is
		// nonfull 	
		ASSERT (!outputQueue -> isFull());
		outputQueue -> enqueue (minusElement);

		//HR implementation by Lory Al Moakar
		// increment the number of tuples outputted so far
		n_tuples_outputted +=  1;
		// end of part 6 of HR implementation by LAM

		lastOutputTs = minusElement.timestamp;

		// delete this element from our synopsis
		rc = outputSynopsis -> deleteTuple (synTuple);
		if (rc != 0) return rc;
	}

	else {		
		// Update the count
		minusEval -> eval ();		
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
bool Distinct::calculateLocalStats() 
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

    avg_k = (1-ALPHA) * avg_k +  ALPHA * current_avg;

    k_measurements += 1;
    M_k = M_k_1 + ( current_avg - M_k_1)/k_measurements;
    S_k += ( current_avg - M_k_1 ) * ( current_avg - M_k );
    M_k_1 = M_k;
    
    stdev = sqrt ( S_k / ( k_measurements -1 ) ) ;
    
    if ( (current_avg - avg_k) > OULIER_FACTOR*stdev) {
      //printf("OUTLIER current %f  new avg %f old avg %f \n", current_avg, local_cost_per_tuple,
      //     old_local_cost);
      local_cost_per_tuple  = old_local_cost;
      
    }
    
    //printf ( "DISTINCT OPERATOR %d costs %llu  n_tuples %d smoothed %f \n",
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

void Distinct::refreshPriority()
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
      if ( top_queue->operator_type == OUTPUT ||
	   top_queue->operator_type == SINK ||
	   top_queue->firstRefresh == true){
         new_mean_selectivity += local_selectivity;

	 }
      else if ( !(top_queue->operator_type == BIN_JOIN ||
		  top_queue->operator_type == BIN_STR_JOIN ) ){
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
	       outputs[0]->operator_type == BIN_STR_JOIN)  )     
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
int Distinct::readyToExecute() {
  if ( outputQueue->isFull() ) 
    return 0;
  return inputQueue->size();
}

// end of part 1 of HR with ready by LAM

int Distinct::run_with_shedder (TimeSlice timeSlice)
{
	return 0;
}

//ArmaDILoS, by Thao Pham
void Distinct::deactivate(){

	status = INACTIVE;

	Element e;
	while(!inputQueue->isEmpty()){
		inputQueue->dequeue(e);
		if(e.tuple){
			UNLOCK_INPUT_TUPLE(e.tuple);
		}
	}

	Tuple t;
	if(outputSynopsis)
		((RelationSynopsis*)outputSynopsis)->clearSyn(outStore);

	if(outputQueue->isEmpty()){
		for(int i=0;i<numOutputs;i++){
			outputs[i]->deactivate();
		}
	}
	resetLocalStatisticsComputationCycle();
	local_cost_per_tuple = 0;
}

//end of ArmaDILoS
