/**
 * @file       lottery_sched_id.cc
 * @date       Sept 25, 2008
 * @brief      Implementation of the lottery scheduler
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

#ifndef _LOTTERY_SCHEDULER_ID
#include "execution/scheduler/lottery_sched_id.h"
#endif

using namespace Execution;

static const TimeSlice timeSlice = 100000;

// the default value for cycle length in tuples.
static const int CYCLE_LENGTH_D = 50;

LotterySchedulerID::LotterySchedulerID (){
  
  this -> numOps = 0;	
  bStop = false;
  // Response Time Calculation By Lory Al Moakar
  //initialize time_unit to 1 if not specified by user
  // or not using server impl to start the system
  time_unit = 1;
  //end of part 1 of response time calculation by LAM
  //initialize max_op_priorities to 0.0
  max_op_priorities = 0.0;

  //set the default cycle length 
  cycle_length = CYCLE_LENGTH_D ;
  
}		
LotterySchedulerID::~LotterySchedulerID () {  }
		
		
int LotterySchedulerID::addOperator (Operator *op) { 
  if (numOps == MAX_OPS)
    return -1;	
  //set n_tuples_inputted to 0 so that all operators start up 
  //with zero
  op -> n_tuples_inputted = 0;
  ops [numOps ++] = op;
  
  return 0;

}

	       	
int LotterySchedulerID::run (long long int numTimeUnits) { 
  
  //declare a response variable to return in case of errors
  int rc = 0;

  //declare a variable that counts the number of tuples processed 
  //in this cycle
  int tuples_in_cycle = 0;
  //for debugging 
  int n_refreshes = 0;



  max_op_priorities = ops[0] -> priority;
  //find the max of priorities in order to assign tickets
  for (unsigned int o = 0 ; o < numOps ; o++) {
    ops[o] -> priority = 1;
    if ( o > (numOps/2)) ops[o] -> priority = 2;
    if ( ops[o] -> priority > max_op_priorities)
      max_op_priorities = ops[o] -> priority; 
 } 

  //assign tickets to each operator
  for (unsigned int o = 0 ; o < numOps ; o++) {
    ops[o]->new_n_tickets = (int)((ops[o] -> priority / max_op_priorities)  * 100); 
    ops[o]->old_n_tickets = (int)((ops[o] -> priority / max_op_priorities)  * 100); 
    
    //add a number of tickets proportional to ops[o] newly calculated number of tickets
    for ( unsigned int c = 0; c < ops[o]->new_n_tickets; c++ )
      tickets.push_back(o);
    

  }
   
  printf("Printing priorities:\n");
  for (unsigned int o = 0 ; o < numOps ; o++) {
    printf ( "op: %d \t n_tickets: %d \n", o, ops[o]->new_n_tickets); 
  }
  
  

  // Response Time Calculation By Lory Al Moakar
  // get the time when the system starts execution
  Monitor::Timer mytime;
  this -> startingTime = mytime.getTime(); 
  //a variable used to store current time when needed
  unsigned long long int curTime = 0;
  
  //feed starting time as a seed to the random number generator
  srand(this -> startingTime); 


  //initialize the system_start_time in each of the operators
  for (unsigned int o = 0 ; o < numOps ; o++) {
    ops [o] -> system_start_time = this -> startingTime;
    ops [o] -> time_unit = this -> time_unit;
  }
  
  
  //end of part 2 of response time calculation by LAM
  
  // numtimeunits == 0 signal for scheduler to run forever (until stopped)
  if (numTimeUnits == 0) {		
    while (!bStop) {
      
      //generate a ticket number
      int ticket_number = (int)(( tickets.size() )*(rand()/(RAND_MAX + 1.0))); 
      //while  ticket number falls out of range
      while ( ticket_number >= tickets.size() )
	//generate another number
	ticket_number = (int)(( tickets.size() )*(rand()/(RAND_MAX + 1.0))); 
      //if the ticket does not belong to any operator
      while ( tickets[ticket_number] == -1 ){
	//generate a new one
	ticket_number = (int)(( tickets.size() )*(rand()/(RAND_MAX + 1.0))); 
	while ( ticket_number >= tickets.size() )
	  ticket_number = (int)(( tickets.size() )*(rand()/(RAND_MAX + 1.0))); 
      }
      int found = tickets[ticket_number];
      
      // run the operator with the winning ticket
      if ((rc = ops [found] -> run(timeSlice)) != 0) {
	return rc;
      	}
    
      //calculate how many tuples have been processed inputted so far in this cycle
      tuples_in_cycle += ops[found]->n_tuples_inputted;
      ops[found]->n_tuples_inputted = 0;
      //Is this cycle over ?
      if ( tuples_in_cycle >= cycle_length ) {
	//reset the number of tuples
	tuples_in_cycle = 0;

	max_op_priorities = ops[0] -> priority;
	//find the max of priorities in order to assign tickets
	for (unsigned int o = 0 ; o < numOps ; o++) {
	  if ( ops[o] -> priority > max_op_priorities)
	    max_op_priorities = ops[o] -> priority; 
	}
      
	//clear the tickets vector
	//tickets.clear();

	
	//regenerate tickets
	int count = 0;
	for (unsigned int o = 0 ; o < numOps ; o++) {
	  ops[o]->new_n_tickets = (int)((ops[o] -> priority / max_op_priorities)  * 100); 
	  ops[o]->old_n_tickets = (int)((ops[o] -> priority / max_op_priorities)  * 100); 
	  
	  //add a number of tickets proportional to ops[o] newly calculated number of tickets
	  for ( unsigned int c = 0; c < ops[o]->new_n_tickets; c++ ) {
	    if ( count < tickets.size() ) 
	      tickets [count] =  o;
	    
	    else 
	      tickets.push_back(o);
	    ++count;
	    
	  } //end of loop that adds tickets

	}
	
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
    
    
    do {
      //generate a ticket number
      int ticket_number = (int)(( tickets.size() )*(rand()/(RAND_MAX + 1.0))); 
      //while  ticket number falls out of range
      //while ( ticket_number >= tickets.size() )
	//generate another number
      // 	ticket_number = (int)(( tickets.size() )*(rand()/(RAND_MAX + 1.0))); 
      //if the ticket does not belong to any operator
      //while ( tickets[ticket_number] == -1 ){
	//generate a new one
      //	ticket_number = (int)(( tickets.size() )*(rand()/(RAND_MAX + 1.0))); 
      //	while ( ticket_number >= tickets.size() )
      //	  ticket_number = (int)(( tickets.size() )*(rand()/(RAND_MAX + 1.0))); 
      // }
      int found = tickets[ticket_number];
      
     
      
      //run the operator with the winning ticket
      if ((rc = ops [found] -> run(timeSlice)) != 0) {
	return rc;
      }

      
      //calculate how many tuples have been processed inputted so far in this cycle
      tuples_in_cycle += ops[found]->n_tuples_inputted;
      ops[found]->n_tuples_inputted = 0;
      
      //Is this cycle over ?
      if ( tuples_in_cycle >= cycle_length ) {
	//reset the number of tuples
	tuples_in_cycle = 0;

	max_op_priorities = ops[0] -> priority;
	//find the max of priorities in order to assign tickets
	for (unsigned int o = 0 ; o < numOps ; o++) {
	  if ( n_refreshes %2 == 0 ){
	    ops[o] -> priority = 1;
	    if ( o > (numOps/2)) ops[o] -> priority = 2;
	  }
	  else {
	    ops[o] -> priority = 2;
	    if ( o > (numOps/2)) ops[o] -> priority = 1;
	  }
	  if ( ops[o] -> priority > max_op_priorities)
	    max_op_priorities = ops[o] -> priority; 
	}
      
	//clear the tickets vector
	//tickets.clear();

	
	//recalculate the number of tickets
	int count = 0;
	for (unsigned int o = 0 ; o < numOps ; o++) {
	  ops[o]->new_n_tickets = (int)((ops[o] -> priority / max_op_priorities)  * 100); 
	  ops[o]->old_n_tickets = (int)((ops[o] -> priority / max_op_priorities)  * 100); 
	  
	  //add a number of tickets proportional to ops[o] newly calculated number of tickets
	  for ( unsigned int c = 0; c < ops[o]->new_n_tickets; c++ ){
	    if ( count < tickets.size() )
	      tickets[count] = o;
	    else 
	      tickets.push_back(o);
	    ++ count;
	  }// end of for that adds tickets
	  
	}
	//increment the number of times we refreshed the tickets
	++n_refreshes;
	printf("Printing priorities:\n");
	for (unsigned int o = 0 ; o < numOps ; o++) {
	  printf ( "op: %d \t n_tickets: %d \n", o, ops[o]->new_n_tickets); 
	}
      }

    
       curTime = mytime.getTime();
       curTime -= this->startingTime;
       curTime = curTime/time_unit;
     } while (!bStop && curTime < numTimeUnits); 
     //end of part 3 of response time calculation by LAM
  }
  
  //printf("n-refreshes = %d\n", n_refreshes);
  //printf("ran for %d timeunits", curTime);
  return 0; 
  
}
int LotterySchedulerID::stop () { 
  bStop = true;
  return 0; 

}
int LotterySchedulerID::resume () { 
  bStop = false;
  return 0; 
}
