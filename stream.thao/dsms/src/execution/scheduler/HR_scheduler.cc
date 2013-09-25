/**
 * @file       HR_scheduler.cc
 * @date       April 23, 2009
 * @brief      Implementation of HR scheduler as described in paper
 * @author     Lory Al Moakar
 */
#include <iostream>
//to generate random numbers
#include <cstdlib> 
#include <stdio.h>

using namespace std;

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _HR_SCHEDULER_
#include "execution/scheduler/HR_scheduler.h"
#endif

// we need to distinguish output operators 
#ifndef _OUTPUT_
#include "execution/operators/output.h"
#endif


#include <math.h>
#include <algorithm>


using namespace Execution;

static const TimeSlice timeSlice = 100000;//100;//100000;

// the default value for cycle length in tuples.
static const int CYCLE_LENGTH_D = 200;

//the starting priority value
static const double STARTING_PRIORITY = 10.0;




HRScheduler::HRScheduler (){
 
  this -> numOps = 0;	
  this -> numSrOps = 0;

  bStop = false;
  // Response Time Calculation By Lory Al Moakar
  //initialize time_unit to 1 if not specified by user
  // or not using server impl to start the system
  time_unit = 1;
  //end of part 1 of response time calculation by LAM
  //initialize max_op_priorities to 0.0

  STEADY_STATE= 0; 
  //set the default cycle length 
  cycle_length = CYCLE_LENGTH_D ;
  
  //load manager, by Thao Pham
  this->num_loadMgrs = 0;
  //end of load manager, by Thao Pham
}		
HRScheduler::~HRScheduler () {  }
		
		
int HRScheduler::addOperator (Operator *op) { 
  if ( (numOps+numSrOps) == MAX_OPS)
    return -1;	
  //set n_tuples_inputted to 0 so that all operators start up 
  //with zero
  op -> n_tuples_inputted = 0;
  op -> stdev = 0;
  op -> avg_k = 0;
  //if the operator is  a source operator --> add to sr_ops
  if ( op->operator_type == STREAM_SOURCE 
       || op->operator_type == REL_SOURCE
       || op->operator_type == SYS_STREAM_GEN ) {
    sr_ops.push_back(op);
    ++numSrOps;
  }
  else {
    //this is a non-source operator --> 
    //add the operator to the ops list
    ops.push_back(op);
    ++numOps;
  }
  return 0;

}

	       	
int HRScheduler::run (long long int numTimeUnits) { 
  
  //printf("\nSCHEDULER STARTED \n");
  //declare a response variable to return in case of errors
  int rc = 0;

  //declare a variable that counts the number of tuples processed 
  //in this cycle
  int tuples_in_cycle = 0;
  //for debugging 
  int n_refreshes = 0;
  int n_ready = 0;
  int n_not_ready = 0;
  //fprintf(stderr, "before loop 1");
  //initialize the priorities of the operators to starting_priority 
  for (unsigned int o = 0 ; o < numOps ; o++) {
    //We start scheduling in a round robin fashion
    ops[o] -> priority = STARTING_PRIORITY;
    ops[o] -> n_refreshes = 0;
    
  } 
  //fprintf(stderr,"before loop 2");
  // Response Time Calculation By Lory Al Moakar
  // get the time when the system starts execution
  Monitor::Timer mytime;
  this -> startingTime = mytime.getTime(); 
  //a variable used to store current time when needed
  long long int curTime = 0;
  
  //feed starting time as a seed to the random number generator
  srand(this -> startingTime); 

  //fprintf(stderr,"before loop 3");
  //initialize the system_start_time in each of the operators
  for (unsigned int o = 0 ; o < numOps ; o++) {
    ops [o] -> system_start_time = this -> startingTime;
    ops [o] -> time_unit = this -> time_unit;
    
  }
  //fprintf(stderr,"before loop 4");
  for ( unsigned int i = 0; i < numSrOps; i++ )  {
    sr_ops [i] -> system_start_time = this -> startingTime;
    sr_ops [i] -> time_unit = this -> time_unit;
    sr_ops[i] -> priority = STARTING_PRIORITY;
    sr_ops[i] -> n_refreshes = 0;
  }
  //end of part 2 of response time calculation by LAM
  //fprintf(stderr,"before loop 5");
  // if the user wishes to calculate stats even before system reaches steady state
  if ( STEADY_STATE == 0 ) {
    for (unsigned int o = 0 ; o < numOps ; o++) {
      if ( ops[o]->operator_type == OUTPUT) {
	((Execution::Output * )(ops[o]))-> steady_state = true;
      }
    }
  }
    
  // numtimeunits == 0 signal for scheduler to run forever (until stopped)
  if (numTimeUnits == 0) {		
    while (!bStop) {
      
      //start from the beginning of the list
      //since it is sorted, we are starting from the operator with the highest 
      //priority 
      int op_index = 0;
      int source_mode = 0;
      //while the current op is not ready
      while (  ops[op_index] ->readyToExecute() <= 0 ) {
	++op_index;
	++ n_not_ready;
	//none of the operators is ready to execute
	//return error that we reached end of input and that
	// none of the operators is ready to execute
	// this error code is understood by generic_client.cc 
	// The user will receive an error message
	if ( op_index == numOps ) {
	  source_mode = 1;
	  break;
	  //return -1712;
	}
      }
      
      //printf( "Running Operator %d \t", ops[op_index]->operator_id);
      if ( !source_mode ) {
	if ((rc = ops [op_index] -> run(timeSlice)) != 0) {
	  return rc;
	}
      }
      //if we need more tuples
      else {
	for ( int i = 0; i < numSrOps; i++ ) {
	  if ((rc = sr_ops[i] -> run(timeSlice)) != 0) 
	    return rc;
	  tuples_in_cycle += sr_ops[i]->n_tuples_inputted;
	  sr_ops[i]->n_tuples_inputted = 0;
	}
      }
      curTime = mytime.getTime();
      curTime -= this->startingTime;
      curTime = (int)((double)curTime/(double)time_unit);
      
      
      //printf("ran for %llu timeunits started at %llu", curTime, startingTime);
      
      
      
      //calculate how many tuples have been inputted so far in this cycle
      //tuples_in_cycle += ops[op_index]->n_tuples_inputted;
      //ops[op_index]->n_tuples_inputted = 0;
      //Is this cycle over ?
      if ( tuples_in_cycle >= cycle_length ) {
	//reset the number of tuples
	tuples_in_cycle = 0;
	
	// refresh the priorities of the operators
	// and this method also resorts the ops vector 
	// by those new priorities
	refreshPriorities();
	
	//increment n_refreshes so that we can keep track 
	// of the number of times the statistics were refreshed.
	++n_refreshes;
	//printf("Printing priorities:\n");
	//for (unsigned int o = 0 ; o < numOps ; o++) {
	//  printf ( "op: %d \t n_tickets: %d \n", o, n_tickets[o]); 
	//}
      }
    }
  }
  
  else {		
    // Response Time Calculation By Lory Al Moakar
    //for (long long int t = 0 ; (t < numTimeUnits) && !bStop ; t++) {
    //get the current time
    Monitor::Timer mytime;
    curTime = mytime.getTime();
    curTime = this->startingTime;
    //fprintf(stderr,"HERERERE");
    do {
      //start from the beginning of the list
      //since it is sorted, we are starting from the operator with the highest 
      //priority 
      int op_index = 0;
      int source_mode = 0;
      //fprintf(stderr,"before loop %d", numOps);
      //while the current op is not ready
      while (  ops[op_index] ->readyToExecute() <= 0 ) {
	++op_index;
	++ n_not_ready;
	//fprintf(stderr,"op index is %d\t",op_index); 
	//none of the operators is ready to execute
	//return error that we reached end of input and that
	// none of the operators is ready to execute
	// this error code is understood by generic_client.cc 
	// The user will receive an error message
	if ( op_index == numOps ) {
	  source_mode = 1;
	  //fprintf(stderr, "SOURCE MODE %d", op_index);
	  break;
	  //return -1712;
	}
      }
      //printf("after loop");
      //printf( "Running Operator %d\n", ops[op_index]->operator_id);
      if ( !source_mode ) {
	if ((rc = ops [op_index] -> run(timeSlice)) != 0) {
	  return rc;
	}
      }
      //if we need more tuples
      else {
	//printf("SOURCE MODE");
	for ( int i = 0; i < numSrOps; i++ ) {
	  if ((rc = sr_ops[i] -> run(timeSlice)) != 0) 
	    return rc;
	  //calculate how many tuples have been processed inputted so far in this cycle
	  tuples_in_cycle += sr_ops[i]->n_tuples_inputted;
	  sr_ops[i]->n_tuples_inputted = 0;
	}
	//Is this cycle over ?
	if ( tuples_in_cycle >= cycle_length ) {
	  //reset the number of tuples
	  tuples_in_cycle = 0;
	  
	  // refresh the priorities of the operators
	  // and this method also resorts the ops vector 
	  // by those new priorities
	  refreshPriorities();
	
	  //increment n_refreshes so that we can keep track 
	  // of the number of times the statistics were refreshed.
	  ++n_refreshes;
	  
	  //printf("Printing priorities:\n");
	  //for (unsigned int o = 0 ; o < numOps ; o++) {
	  // printf ( "op: %d \t n_tickets: %d priority %f Type %d \n", o, n_tickets[o]
	  //   , ops[o]->priority, ops[o]->operator_type); 
	  //}
	  
	  // find out if the system is in steady state 
	  if ( steadySystem()){
	    //printf("SYSTEM OPERATING IN STEADY STATE ");
	    for (unsigned int o = 0 ; o < numOps ; o++) {
	      if ( ops[o]->operator_type == OUTPUT) {
		((Execution::Output * )(ops[o]))-> steady_state = true;
	      }
	    }
	  }
	}
      
      }
      
      
      //printf("now running %d\t", op_index);
      //printf("ran for %llu timeunits started at %llu", curTime, startingTime);
      
      //calculate how many tuples have been processed inputted so far in this cycle
      //tuples_in_cycle += ops[op_index]->n_tuples_inputted;
      //ops[op_index]->n_tuples_inputted = 0;
      
      
      
    
    
      curTime = mytime.getTime();
      curTime -= this->startingTime;
      curTime = curTime / time_unit;
      //curTime = (long long int)((double)curTime/(double)time_unit);
      if ( curTime < 0 )  { 
	
	printf("ERROR GOING BACK IN TIME %lld", curTime);
	curTime = mytime.getTime();
	printf("now is %lld start was %lld", curTime,this->startingTime );
	return -1656;
      }
      
      if ( curTime >= numTimeUnits){
	printf( "Time ended %lld unit is %d \n",  
		numTimeUnits, time_unit);
	
      }
      if(bStop) printf("Mystereous stop");
      
    } while (!bStop && curTime < numTimeUnits); 
    //end of part 3 of response time calculation by LAM
  }
  
 //printf("n-refreshes = %d\n", n_refreshes);
  printf("ran for %llu timeunits started at %llu", curTime, startingTime);
  printf(" Now is : %llu \n", mytime.getTime());
  printf( "ready ops is %d Not ready ops is %d \n", n_ready, n_not_ready );
  refreshPriorities();
  printf("Printing priorities:\n");
  for (unsigned int o = 0 ; o < numOps ; o++) {
    printf ( "op: %d \t  priority %f Type %d selectivity %f cost %f\n",
	     ops[o]->operator_id, ops[o]->priority, ops[o]->operator_type,
	     ops[o]-> local_selectivity, ops[o]->local_cost_per_tuple);
  }
  return 0; 
  
}
int HRScheduler::stop () { 
  bStop = true;
  return 0; 

}
int HRScheduler::resume () { 
  bStop = false;
  return 0; 
}

//this method is used to compare the operators in order to determine their
//order for the scheduler
bool compare_operators_priority ( Operator * op1 , Operator * op2 ) {
  return ( op1->priority > op2 -> priority );
}


//HR implementation by Lory Al Moakar
		
int HRScheduler::refreshPriorities() {
  
  std::vector <Operator*> opList ; 
  for (unsigned int o = 0 ; o < numOps ; o++) {
    ops[o]-> n_refreshes += ops[o]->calculateLocalStats();
    //printf("op %d tuples %d output %d selectivity %f cost %f\n",
    //   o, 
    //   ops[o]->num_tuples_processed,
    //   ops[o]->n_tuples_outputted,
    //   ops[o]->local_selectivity,
    //   ops[o]->local_cost_per_tuple);
    ops[o]->visited = false;
    if ( ops[o]->operator_type == OUTPUT ||
	 ops[o]->operator_type == SINK ){
      for ( int i = 0; i < ops[o]->numInputs; i++ )
	opList.push_back(ops[o]->inputs[i]);
      ops[o]->visited = true;

      //ops[o]->refreshPriority();

    } //end of if output operator
  } // endof for loop
  
  while( !opList.empty()) {
    //printf("oplist is not empty %d\n", opList.size());
    //for ( int x = 0; x < opList.size(); x++ )
    // printf ( "opType %d \t", opList[x]->operator_type);

    if ( opList[0]->visited == true) {
      opList.erase(opList.begin());
    }
    else if ( opList[0]->operator_type == REL_SOURCE ||
	 opList[0]->operator_type == STREAM_SOURCE ||
	 opList[0]->operator_type == SYS_STREAM_GEN  ) {
      opList[0]->visited = true;
      opList.erase(opList.begin());

    }//end of if operator is an input op
    //is this an operator that is not shared
    else  if ( opList[0]->numOutputs == 1 ) {
      opList[0]->refreshPriority();
      for ( int i = 0; i < opList[0]->numInputs; i++ )
	opList.insert(opList.begin()+1,opList[0]->inputs[i]);
      opList[0]->visited = true;
      opList.erase(opList.begin());



    }//end if operator is not shared
    
    else {
      
      bool outputs_visited = true;
      //have its inputs been visited
      for ( int i = 0; i< opList[0]->numOutputs; i++ ){	
	if ( opList[0]->outputs[i]->visited == false )
	  outputs_visited = false;

      }// end of for loop
      
      if (outputs_visited ) {
	if ( opList[0]-> num_tuples_processed != 0)
	  opList[0]->refreshPriority();
	for ( int i = 0; i < opList[0]->numInputs; i++ )
	  opList.insert(opList.begin()+1,opList[0]->inputs[i]);
	opList[0]->visited = true;
	opList.erase(opList.begin());
	
      }
      else {
       
	opList.push_back(opList[0]);
	opList.erase(opList.begin());
      }
    } // end of operator is not shared
  }//end of while  opList !empty

  //call the method to assign dynamic priorities to 
  //source and output operators
  assignPriToSrcOut();
  
  //sort the operators in the ops list in decreasing order of priorities
  stable_sort( ops.begin(), ops.end(), compare_operators_priority) ;
  
  return 0;
}

/** 
 * this method is used to assign priorities to source
 * and output operators.
 * @return int 0 success !0 failure
 */
int HRScheduler::assignPriToSrcOut() {
  printf("assigning priorities to Sources\n");
  
  //assign priorities to sources
  for ( unsigned int o = 0; o < numOps; o++ ) {
    if ( ops[o]->operator_type == REL_SOURCE ||
	 ops[o]->operator_type == STREAM_SOURCE ) {
      //printf("op is %d \t ", o);
      //find the highest priority among its outputs
      double max_priority = ops[o]-> outputs[0]->priority;
      //printf( "numOutputs is %d \n",  ops[o]-> numOutputs); 
      if ( ops[o]-> numOutputs > 1 ) 
	for ( int i = 1; i < ops[o]-> numOutputs; i++ ) {
	  //printf ( " i is %d \t", i);
	  if ( ops[o]->outputs[i]->priority > max_priority )
	    max_priority = ops[o]-> outputs[i]->priority;
	}
      
      //assign it the highest priority among its outputs
      ops[o]->priority = 0;//max_priority;
      //printf ( " Max priority for %d is %f ", o, max_priority ); 
      
    }//end of if op is source
    
  }//end of for loop
    
  //assign this range_win_priority to join operators
  for ( unsigned int o = 0; o < numOps; o++ ) {
    if ( (ops[o]->operator_type == BIN_JOIN ||
	  ops[o]->operator_type == BIN_STR_JOIN)
	 && ops[o]->n_refreshes != 0 ){
      //assigning the priority of a join operator to
      //be the max priority among its input operator
      ops[o]->priority = ops[o]->inputs[0]->priority;
      if ( ops[o]->inputs[0]->priority < ops[o]->inputs[1]->priority ) {
	ops[o]->priority = ops[o]->inputs[1]->priority;
	ops[o]->inputs[0]->priority = ops[o]->inputs[1]->priority;
      }
      else
	ops[o]->inputs[1]->priority = ops[o]->inputs[0]->priority;
      
      //ops[o] -> priority = ops[o]->priority * 2;
      //  printf ( "PR op %d %f \n", o,  ops[o]->priority);
    }
  }

  //assign this join priority to istream  operators
  for ( unsigned int o = 0; o < numOps; o++ ) {
    if ( ops[o]->operator_type == ISTREAM && 
	 ops[o]->n_refreshes != 0 ){
      //assigning the priority of an istream  operator to
      //be the priority of its input operator
      ops[o]->priority = ops[o]->inputs[0]->priority;
      //  printf ( "PR op %d %f \n", o,  ops[o]->priority);
    }
    }

  


  //assign this max_priority to output operators
  for ( unsigned int o = 0; o < numOps; o++ ) {
    if ( ops[o]->operator_type == OUTPUT ||
	 ops[o]->operator_type == SINK  ){
      //assigning the priority of an output operator to
      //be the priority of its input operator
      ops[o]->priority = ops[o]->inputs[0]->priority;
      //printf ( "PR op %d %f \n", o,  ops[o]->priority);
    }
    // EXAMINE THIS LATER *********************************
    if (ops[o]->operator_type == SYS_STREAM_GEN) {
      //double min_priority = ops[0]-> priority;
      //for (int i =1; i< numOps; i++ )
      //if ( ops[i]->priority < min_priority)
      //  min_priority =  ops[i]->priority;
      
      ops[o]->priority = 0;//min_priority;
    }
  }//end of third for loop


  return 0;

}


// end of part 1 of HR implementation by LAM


void HRScheduler::adjustPrioritiesOfOutputs ( Operator * currentOp, double multiplier ) {
  //printf("call to adjust priorities of outputs");
  if ( currentOp -> operator_type == OUTPUT || 
       currentOp -> operator_type == SINK ) 
    return;
  currentOp -> priority *= multiplier;
  multiplier *= currentOp->local_selectivity / currentOp -> local_cost_per_tuple;
  
  for ( int i = 0; i < currentOp -> numOutputs; i++ )
    adjustPrioritiesOfOutputs ( currentOp ->outputs[i],multiplier );


}



/**
 * this method goes over the operators
 * and finds out if the system is in steady 
 * state or not
 */
bool HRScheduler::steadySystem () {
  
  for (unsigned int o = 1 ; o < numOps ; o++) {
    if ( ops[o]-> n_refreshes < STEADY_STATE && !(
         ops[o]->operator_type == OUTPUT || 
	 ops[o]->operator_type == REL_SOURCE ||
	 ops[o]->operator_type == SINK || 
	 ops[o]->operator_type == STREAM_SOURCE ||
	 ops[o]->operator_type == SYS_STREAM_GEN )){
      printf ( "not ready op is %d n_refreshes %d\n ", 
            ops[o]-> operator_id, ops[o]-> n_refreshes);    
      return false;
    }
  }
  return true;
}
 
