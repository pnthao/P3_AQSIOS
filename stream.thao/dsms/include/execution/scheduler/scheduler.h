#ifndef _SCHEDULER_
#define _SCHEDULER_

 #include <iostream>
#include <stdlib.h> 
#include <math.h>

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif

// Response Time Calculation By Lory Al Moakar
#ifndef _TIMER_
#include "execution/monitors/timer.h"
#endif
//end of part 1 of response time calculation by LAM

//load manager, by Thao Pham
#ifndef _LOAD_MGR_
#include "execution/loadmanager/load_mgr.h"
#endif

#ifndef _CONSTANTS_
#include "common/constants.h"
#endif

#include<set>
#include<map>

//#ifndef _PARAMS_
//#include "server/params.h"
//#endif
extern double HEADROOM_FACTOR;
//end of part 1 of load mamaging, by Thao Pham
using namespace std;
namespace Execution {
	class Scheduler {
	public:
		//Amardilos
		//get the list of file pos of the sources corresponding to a list of query ID,
		//the related schedulers is expected to provide an override of this method
		virtual void getSourceFilePos(std::set<int> queryIDs,std::map<Operator*,streampos> &sourceFilePos){};

		/**
		 * Add a new operator to schedule
		 */
		virtual int addOperator (Operator *op) = 0;
		
		/**
		 * Schedule the operators for a prescribed set of time units.
		 */		
		virtual int run(long long int numTimeUnits) = 0;
		
		virtual int stop () = 0;
		virtual int resume () = 0;
		
		// Response Time Calculation By Lory Al Moakar
		// A variable used to store the time the scheduler
		// started executing
		unsigned long long int startingTime;

		//added by Thao Pham, a viariable to keep track of CPU start time
		unsigned long long int startingCPUTime;

		// a variable used to store the length of 
		// a time unit
		int time_unit;
		//end of part 2 of response time calculation by LAM
					
		// an array that holds the priorities of the query classes
		// as inputted by the user
		
		//Query Class Scheduling by Lory Al Moakar
	// the maximum number of query classes in the system
		static const int MAX_QUERY_CLASSES =  10 ;
		double query_class_priorities [MAX_QUERY_CLASSES];
		double original_query_class_priorities[MAX_QUERY_CLASSES];
		// the number of query classes inputted by the user
		int n_query_classes;
		//end of Query Class Scheduling by LAM
		
		//Load manager, by Thao Pham
		LoadManager* loadMgrs [MAX_QUERY_CLASSES]; //we have separate load managers for each of the class
		unsigned int num_loadMgrs;
		
		int addLoadManager(LoadManager* mgr)
		{
			loadMgrs[num_loadMgrs++] = mgr;
			return 0;
		}
		//for the control-based load shedder
		long long unsigned int ctrl_last_ts;
		
		//end of part 2 of load manager, by Thao Pham
		
		//load manager synergy, by Thao Pham
		int count_loadManagementCycle;
		
		void adjustPriority(int k, double amount)
		{
			//amount: percentage of system capacity that need to add/remove, in %
			
			double total_priorities = 0;
		  	for ( int i = 0; i < n_query_classes; i++ ) 
		    		total_priorities +=  original_query_class_priorities[i];
			
			double new_pri = query_class_priorities[k] + (total_priorities*amount)/100;
			//adjust the headroom factor

			//loadMgrs[k]->headroom_factor = ((new_pri/query_class_priorities[k])*loadMgrs[k]->headroom_factor
			//		< HEADROOM_FACTOR*((double)query_class_priorities[k])/total_priorities)?
			//				((new_pri/query_class_priorities[k])*loadMgrs[k]->headroom_factor)
			//				:(HEADROOM_FACTOR*((double)query_class_priorities[k])/total_priorities);
			//loadMgrs[k]->headroom_temp = (new_pri/query_class_priorities[k])*loadMgrs[k]->headroom_factor;			
			//loadMgrs[k]->headroom_factor =  HEADROOM_FACTOR*((double)query_class_priorities[k])/total_priorities;

			loadMgrs[k]->headroom_factor = (new_pri/query_class_priorities[k])*loadMgrs[k]->headroom_factor;	

			query_class_priorities[k] = new_pri;
			
			
		}
		
		double getCurrentPriority(int k)
		{
			double total_priorities = 0;
		  	for ( int i = 0; i < n_query_classes; i++ ) 
		    		total_priorities +=  original_query_class_priorities[i];
			//printf("total: %f\n", total_priorities);
			return (query_class_priorities[k]/total_priorities)*100; 
		}

		double getOriginalPriority(int k)
		{	
			double total_priorities = 0;
		  	for ( int i = 0; i < n_query_classes; i++ ) 
		    		total_priorities +=  original_query_class_priorities[i];
			//printf("total: %f\n", total_priorities);
			return (original_query_class_priorities[k]/total_priorities)*100;
		}

		void ResourceBalancing()
		{
			HEADROOM_FACTOR = 0;
			for(int i=0; i< num_loadMgrs; i++)
				HEADROOM_FACTOR += loadMgrs[i]->headroom_factor;

			double budget = 0;
			double demands[MAX_QUERY_CLASSES];
			
			//FISRT PASS: calculating avalaible budget
			for(int i=0;i<num_loadMgrs; i++)
			{
				if(loadMgrs[i]->avg_capacity_usage<=100)
				{	
									
					//printf("%d:: %f:: %f\n",i, loadMgrs[i]->avg_capacity_usage, getCurrentPriority(i));
					double supply = ((100-loadMgrs[i]->avg_capacity_usage)*getCurrentPriority(i))/100; 
					budget += supply;
					//printf("capacity usage: %f , current priority: %f , demand: %f \n", loadMgrs[i]->avg_capacity_usage, getCurrentPriority(i), demands[i]);		
					//take the redundant capacity from the class
					adjustPriority(i,-supply);
					demands[i] = 0;
				}
				else{
					demands[i] = ((loadMgrs[i]->avg_capacity_usage-100)*getCurrentPriority(i))/100;	
					//printf("capacity usage: %f , current priority: %f , demand: %f \n", loadMgrs[i]->avg_capacity_usage, getCurrentPriority(i), demands[i]);		
				}
							
				loadMgrs[i]->resetCapacityUsageTracking();
			}
			//printf("%0.1f \n", budget);
			//SECOND PASS: consider those that are running under its original priority and demanding more
			for(int i=0;i<num_loadMgrs;i++)
				if(demands[i]>0 && query_class_priorities[i]<original_query_class_priorities[i])
				{
					double oriGap = getCurrentPriority(i)*(original_query_class_priorities[i]
														/query_class_priorities[i] -1);
					if(demands[i] < oriGap)
					{
						adjustPriority(i,demands[i]);
						budget -=demands[i];
						demands[i] = 0;
					}
					else
					{
						query_class_priorities[i] = original_query_class_priorities[i];
						budget -= oriGap;
						demands[i] -=oriGap;
					}
				}
			
			//THIRD PASS: if budget > 0, consider classes that are still demand more capacity, from high to low priority
			if(budget>0){
				for (int i=0;i<num_loadMgrs;i++)
				{
					if(demands[i]>0){
						if(demands[i]> budget){
							adjustPriority(i,budget);
							demands[i] -= budget;
							budget = 0;
							break;
						}
						else{
							adjustPriority(i,demands[i]);
							budget -=demands[i];
							demands[i]=0;
						}	
					}
					if(budget <=0.001) break;
				}
				//if the budget is still >0, consider giving the budget to those class still running under priority
				if(budget>0)
				{
					for(int i=0;i<num_loadMgrs;i++)
					{
						if(query_class_priorities[i]<original_query_class_priorities[i])
						{
							double oriGap = getCurrentPriority(i)*(original_query_class_priorities[i]
															/query_class_priorities[i] -1);
							if(oriGap < budget)
							{
								query_class_priorities[i] = original_query_class_priorities[i];
								budget -= oriGap;
							}
							else
							{
								adjustPriority(i,budget);
								budget =0;
							}
						
						}
						if(budget<=0.001) break;	
					}	
				}		
			}
			//FORTH PASS: if budget is less than 0
			else //budget < 0
			{
				for (int i=num_loadMgrs-1;i>=0; i--) //low to high priority
				{
					if(query_class_priorities[i]>original_query_class_priorities[i])
					{
						double oriGap = getCurrentPriority(i)*(1- original_query_class_priorities[i]
														/query_class_priorities[i]);
						if(oriGap > (-budget))
						{
							adjustPriority(i,budget);
							budget = 0;
							break;
						}
						else
						{
							query_class_priorities[i] = original_query_class_priorities[i];
							budget += oriGap;
						}
					}
					if (budget>=-0.001) break;
				}
					
			}
				
			
		}
void ResourceBalancing_G()
		{
			
			HEADROOM_FACTOR = 0;
			for(int i=0; i< num_loadMgrs; i++)
				HEADROOM_FACTOR += loadMgrs[i]->headroom_factor;
			
			double budget = 0;
			double demands[MAX_QUERY_CLASSES];
			/*for(int i=0;i<num_loadMgrs;i++)	
			{
				printf("%d: %f :%f,  ", i, loadMgrs[i]->avg_capacity_usage, loadMgrs[i]->headroom_factor);
			}*/
			//FISRT PASS: calculating avalaible budget
			for(int i=0;i<num_loadMgrs; i++)
			{
				if(loadMgrs[i]->avg_capacity_usage<=100) 
				{	
					//if the query class is running at or under its original priority, leave a little bit of its redundant capacity
					double supply = ((100-loadMgrs[i]->avg_capacity_usage)*getCurrentPriority(i))/100; 
					
					if(query_class_priorities[i]<=original_query_class_priorities[i])
					{
						supply = supply-getOriginalPriority(i)*0.05;	 
					}
					if(supply >0)
					{	budget += supply;
						//printf("capacity usage: %f , current priority: %f , demand: %f \n", loadMgrs[i]->avg_capacity_usage, getCurrentPriority(i), demands[i]);		
						//take the redundant capacity from the class
						adjustPriority(i,-supply);
						demands[i] = 0;
					}
				}
				else{
					demands[i] = ((loadMgrs[i]->avg_capacity_usage-100)*getCurrentPriority(i))/100;	
					//printf("capacity usage: %f , current priority: %f , demand: %f \n", loadMgrs[i]->avg_capacity_usage, getCurrentPriority(i), demands[i]);		
				}
							
				//(we reset later after updating the node info)
				//loadMgrs[i]->resetCapacityUsageTracking();

			}
			//printf("%0.1f \n", budget);
			//SECOND PASS: consider those that are running under its original priority and demanding more
			for(int i=0;i<num_loadMgrs;i++)
				if(demands[i]>0 && query_class_priorities[i]<original_query_class_priorities[i])
				{
					double oriGap = getCurrentPriority(i)*(original_query_class_priorities[i]
														/query_class_priorities[i] -1);
					if(demands[i] + getOriginalPriority(i)*0.05 < oriGap)
					{
						adjustPriority(i,demands[i] + getOriginalPriority(i)*0.05);
						budget -= (demands[i] + getOriginalPriority(i)*0.05);
						demands[i] = 0;
					}
					else
					{
						//query_class_priorities[i] = original_query_class_priorities[i];
						adjustPriority(i,oriGap);
						budget -= oriGap;
						demands[i] -=oriGap;
					}
				}
			
			//THIRD PASS: if budget > 0, consider classes that are still demand more capacity, from high to low priority
			if(budget>0){
				for (int i=0;i<num_loadMgrs;i++)
				{
					if(demands[i]>0){
						if(demands[i]*1.00> budget){
							adjustPriority(i,budget);
							demands[i] -= budget;
							budget = 0;
							for(int j = i;j < num_loadMgrs; j++)
							{
								if(demands[j]>0) 
								{
									int k = num_loadMgrs-1;
									while(k >j && demands[j] > 0.00001)
									{
										if(query_class_priorities[k]>original_query_class_priorities[k])
										{
											double oriGap = getCurrentPriority(k)*(1- original_query_class_priorities[k]
																		/query_class_priorities[k]);
											if(demands[j] <=oriGap)
											{
												adjustPriority(j,demands[j]);
												adjustPriority(k,-demands[j]);
												demands[j] = 0;
											}
											else
											{
												adjustPriority(j,oriGap);
												adjustPriority(k, -oriGap);
												demands[j] -=oriGap;
											}
										
										}
										
										k--;
									}
								}
							}
							
							break;
						}
						else{
							adjustPriority(i,demands[i]*1.00);
							budget -=demands[i]*1.00;
							demands[i]=0;
						}	
					}
					if(budget <=0) break;
				}
				//if the budget is still >0, consider giving the budget to those class still running under priority
				if(budget>0)
				{
					for(int i=0;i<num_loadMgrs;i++)
					{
						if(query_class_priorities[i]<original_query_class_priorities[i])
						{
							double oriGap = getCurrentPriority(i)*(original_query_class_priorities[i]
															/query_class_priorities[i] -1);
							if(oriGap < budget)
							{
								//query_class_priorities[i] = original_query_class_priorities[i];
								adjustPriority(i,oriGap);
								budget -= oriGap;
							}
							else
							{
								adjustPriority(i,budget);
								budget =0;
							}
						
						}
						if(budget<=0) break;	
					}	
				}
						
			}
			//FORTH PASS: if budget is less than 0
			else //budget < 0
			{
				for (int i=num_loadMgrs-1;i>=0; i--) //low to high priority
				{
					if(query_class_priorities[i]>original_query_class_priorities[i])
					{
						double oriGap = getCurrentPriority(i)*(1- original_query_class_priorities[i]
														/query_class_priorities[i]);
						if(oriGap > (-budget))
						{
							adjustPriority(i,budget);
							budget = 0;
							break;
						}
						else
						{
							//query_class_priorities[i] = original_query_class_priorities[i];
							adjustPriority(i,-oriGap);
							budget += oriGap;
						}
					}
					if (budget>=0) break;
				}
					
			}
			
			for(int j = 0;j < num_loadMgrs; j++)
			{
				if(demands[j]>0) 
				{
					int k = num_loadMgrs-1;
					while(k >j && demands[j] > 0.00001)
					{
						if(query_class_priorities[k]>original_query_class_priorities[k])
						{
							double oriGap = getCurrentPriority(k)*(1- original_query_class_priorities[k]
														/query_class_priorities[k]);
							if(demands[j] <=oriGap)
							{
								adjustPriority(j,demands[j]);
								adjustPriority(k,-demands[j]);
								demands[j] = 0;
							}
							else
							{
								adjustPriority(j,oriGap);
								adjustPriority(k, -oriGap);
								demands[j] -=oriGap;
							}
						
						}
						
						k--;
					}
				}
			}
				
			
		}

		void ResourceBalancing_new()
		{
			HEADROOM_FACTOR = 0;
			for(int i=0; i< num_loadMgrs; i++)
				HEADROOM_FACTOR += loadMgrs[i]->headroom_factor;
			double budget = 0;
			double demands[MAX_QUERY_CLASSES];
			
			/*for(int i=0;i<num_loadMgrs;i++)	
			{
				printf("%d: %f :%f,  ", i, loadMgrs[i]->avg_capacity_usage, loadMgrs[i]->headroom_factor);
			}			
			printf("\n");*/
			//FISRT PASS: calculating avalaible budget, demands of each class, all compared to the original priorities
			for(int i=0;i<num_loadMgrs; i++)
			{
				if(query_class_priorities[i]<=original_query_class_priorities[i])
				{
					double oriGap = getCurrentPriority(i)*(original_query_class_priorities[i]
															/query_class_priorities[i] -1);
					
					if(loadMgrs[i]->avg_capacity_usage<=100) 
					{	
						//if the query class is running at or under its original priority, leave a little bit of its redundant capacity
						double supply = ((100-loadMgrs[i]->avg_capacity_usage)*getCurrentPriority(i))/100;
						supply  = supply + oriGap - 0.05*getOriginalPriority(i);
						if(supply > 0){ 
							budget+=supply; //supply from the original level
							adjustPriority(i, oriGap-supply);
						}
						
						demands[i]=0;
					}	
					
					else
					{
						demands[i] = ((loadMgrs[i]->avg_capacity_usage-100)*getCurrentPriority(i))/100;
										
						if(demands[i] <= oriGap) {
							
							double supply = oriGap - demands[i] - 0.05*getOriginalPriority(i);
							if(supply > 0){ 
								budget+=supply; //supply from the original level
								adjustPriority(i, oriGap-supply);
							}
							
						}
						else 
						{
							demands[i] -= oriGap; //demands from the original level
							//query_class_priorities[i] = original_query_class_priorities[i];
							adjustPriority(i,oriGap);
						}
					}
					loadMgrs[i]->resetCapacityUsageTracking();
				}
				
				else //current priority is higher than original one
				{
					double oriGap = getCurrentPriority(i)*(1- original_query_class_priorities[i]
														/query_class_priorities[i]);
					
					if(loadMgrs[i]->avg_capacity_usage <= 100) 
					{	
						double supply = ((100-loadMgrs[i]->avg_capacity_usage)*getCurrentPriority(i))/100;
						if(supply <= oriGap)
						{
							demands[i] = oriGap - supply;
							//query_class_priorities[i] = original_query_class_priorities[i];
							adjustPriority(i,-oriGap);
						}
						else
						{
							supply = supply - oriGap - 0.05*getOriginalPriority(i);
							if(supply > 0){ 
								budget += supply; //supply from the original level
								adjustPriority(i, -(oriGap + supply));
							}
							demands[i] = 0;
						}
						
					}
					else
					{
						demands[i] = ((loadMgrs[i]->avg_capacity_usage-100)*getCurrentPriority(i))/100;
						demands[i] = demands[i] + oriGap;
						//query_class_priorities[i] = original_query_class_priorities[i];
						adjustPriority(i,-oriGap);
					}
					
				
				}
			} // end of first pass
			
			//second pass, deliver the available budgets for those who are demanding more
			//from high to low priority
			if(budget>0){
				for (int i=0;i<num_loadMgrs;i++)
				{
					if(demands[i]>0){
						if(demands[i]/**1.1*/> budget){
							adjustPriority(i,budget);
							demands[i] -= budget;
							budget = 0;
							break;
						}
						else{
							adjustPriority(i,demands[i]/**1.1*/);
							budget -=demands[i]/**1.1*/;
							demands[i]=0;
						}	
					}
					if(budget <=0) break;
				}
				//if the budget is still >0, consider giving the budget to those class still running under priority
				if(budget>0)
				{
					for(int i=0;i<num_loadMgrs;i++)
					{
						if(query_class_priorities[i]<original_query_class_priorities[i])
						{
							double oriGap = getCurrentPriority(i)*(original_query_class_priorities[i]
															/query_class_priorities[i] -1);
							if(oriGap < budget)
							{
								//query_class_priorities[i] = original_query_class_priorities[i];
								adjustPriority(i,oriGap);
								budget -= oriGap;
							}
							else
							{
								adjustPriority(i,budget);
								budget =0;
							}
						
						}
						if(budget<=0) break;	
					}	
				}		
			}
			/*for(int i=0;i<num_loadMgrs;i++)	
			{
				printf("%d: %f", i, query_class_priorities[i]);
			}			
			printf("\n");*/
		}
		
	};
}
		
#endif
