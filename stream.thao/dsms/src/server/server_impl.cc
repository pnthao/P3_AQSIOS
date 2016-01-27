/**
 * @file       server_impl.cc
 * @date       May 22, 2004
 * @brief      Implementation of the STREAM server.
 */

/// debug
#include <iostream>
using namespace std;


#ifndef _ERROR_
#include "interface/error.h"
#endif

#ifndef _DEBUG_
#include "common/debug.h"
#endif

#ifndef _SERVER_IMPL_
#include "server/server_impl.h"
#endif

#ifndef _PARSER_
#include "parser/parser.h"
#endif

#ifndef _NODES_
#include "parser/nodes.h"
#endif

#ifndef _QUERY_
#include "querygen/query.h"
#endif

#ifndef _LOGOP_
#include "querygen/logop.h"
#endif

#ifndef _ROUND_ROBIN_
#include "execution/scheduler/round_robin.h"
#endif

//Lottery scheduling by Lory Al Moakar
#ifndef _LOTTERY_SCHEDULER_
#include "execution/scheduler/lottery_scheduler.h"
#endif

#ifndef _LOTTERY_SCHEDULER_ID
#include "execution/scheduler/lottery_sched_id.h"
#endif

#ifndef _LOAD_MGR_
#include "execution/loadmanager/load_mgr.h"
#endif
//comment out by Thao Pham, ignore this for now
/*#ifndef _LOTT_SCHED_READY_
#include "execution/scheduler/lott_sched_ready.h"
#endif
// end of part 1 of lottery scheduling by LAM
 */
//HR SCHEDULING by Lory Al Moakar
#ifndef _HR_SCHEDULER_
#include "execution/scheduler/HR_scheduler.h"
#endif
//end of part 1 of HR scheduling by LAM
//Query class Scheduling by Lory Al Moakar
#ifndef _QC_SCHEDULER_
#include "execution/scheduler/QC_scheduler.h"
#endif

#ifndef _WRR_SCHEDULER_
#include "execution/scheduler/WRR_scheduler.h"
#endif
/*#ifndef _QC_WITH_SRCS_
#include "execution/scheduler/QC_with_srcs.h"
#endif

#ifndef _WHR_SCHEDULER_
#include "execution/scheduler/WHR_scheduler.h"
#endif
*/
//end of part 1 of Query class scheduling by LAM

//end of comment out by Thao Pham


#ifndef _CONFIG_FILE_READER_
#include "server/config_file_reader.h"
#endif

#ifndef _PARAMS_
#include "server/params.h"
#endif

#ifdef _DM_
#include "querygen/query_debug.h"
#include "parser/nodes_debug.h"
#include "querygen/logop_debug.h"
#endif

using namespace std;

// Helper functions ...
static int registerRelation (NODE *parseTree,
							 Metadata::TableManager *tableMgr,
							 unsigned int &relId);

static int registerStream (NODE *parseTree,
						   Metadata::TableManager *tableMgr,
						   unsigned int &strId);


ServerImpl::ServerImpl(ostream &_LOG)
	: LOG (_LOG)
{
	state             = S_INIT;
	tableMgr          = 0;
	qryMgr            = 0;
	planMgr           = 0;
	scheduler         = 0;
	//load manager, by Thao Pham
	//loadMgr 		  = 0;
	
	// Set default values of various server params
	MEMORY            = MEMORY_DEFAULT;
	QUEUE_SIZE        = QUEUE_SIZE_DEFAULT;
	SHARED_QUEUE_SIZE = SHARED_QUEUE_SIZE_DEFAULT;
	INDEX_THRESHOLD   = INDEX_THRESHOLD_DEFAULT;
	SCHEDULER_TIME    = SCHEDULER_TIME_DEFAULT;
	CPU_SPEED         = CPU_SPEED_DEFAULT;
	
	// Response Time Calculation By Lory Al Moakar
	TIME_UNIT         = TIME_UNIT_DEFAULT; 
	//end of part 1 of response time calculation by LAM
	
	//load managing, by Thao Pham
	INPUT_RATE_TIME_UNIT = INPUT_RATE_TIME_UNIT_DEFAULT;
	HEADROOM_FACTOR = HEADROOM_FACTOR_DEFAULT;
	DELAY_TOLERANCE = DELAY_TOLERANCE_DEFAULT;
	INITIAL_GAP = INITIAL_GAP_DEFAULT;
	NEXT_GAP = NEXT_GAP_DEFAULT;
	LOAD_MANAGER = LOAD_MANAGER_DEFAULT;
	
	//end of load managing, by Thao Pham
	//Lottery scheduling by Lory Al Moakar
	SCHEDULING_POLICY = ROUND_ROBIN_SCHED;
	// end of part 2 of lottery scheduling by LAM

	
	
	//Query Class Scheduling by Lory Al Moakar
	N_QUERY_CLASSES = N_QUERY_CLASSES_DEFAULT;
	QUERY_CLASS_PRIORITIES[0] = DEFAULT_QC_PRIORITY;
	current_query_class = 1;
	HONOR_PRIORITIES = HONOR_PRIORITIES_DEFAULT;
	STEADY_STATE = STEADY_STATE_DEFAULT;
	QUOTA = QUOTA_DEFAULT;
	SHARING_AMONG_CLASSES = SHARING_AMONG_CLASSES_D;

	//end of part 1 of Query Class Scheduling by LAM
	
	//end of comment out by Thao Pham, ignore these for now
	
	pthread_mutex_init (&mutex, NULL);
	pthread_cond_init (&mainThreadWait, NULL);
	pthread_cond_init (&interruptThreadWait, NULL);
}

ServerImpl::~ServerImpl()
 {
	if (logPlanGen)
		delete logPlanGen;

	if (semInterpreter)
		delete semInterpreter;
	
	if (tableMgr)
		delete tableMgr;
	
	if (planMgr)
		delete planMgr;
	
	if (qryMgr)
		delete qryMgr;

	//load manager, by Thao Pham
	/*if(scheduler->loadMgrs)
		delete scheduler->loadMgrs;
		//end of load manager, by Thao Pham*/
	
	if (scheduler)
		delete scheduler;
		
	
}


/**
 * Begin the specification of an application.
 *
 * Server should be in INIT state when this method id called.  It moves to
 * S_APP_SPEC state when it returns from this method.  
 *
 * Also server starts up:
 *
 * 1. Table manager to manage named streams and relations (input &
 *    derived)
 */

int ServerImpl::beginAppSpecification() 
{
	// State transition
	if(state != S_INIT)
		return INVALID_USE_ERR;
	state = S_APP_SPEC;
	
	// Table manager
	tableMgr = new Metadata::TableManager();
	if(!tableMgr)
		return INTERNAL_ERR;

	// Query Manager
	qryMgr = new Metadata::QueryManager(LOG);
	if (!qryMgr)
		return INTERNAL_ERR;
	
	// Plan manager
	planMgr = Metadata::PlanManager::newPlanManager(tableMgr, LOG);
	if(!planMgr)
		return INTERNAL_ERR;
	
	
	planMgr-> sharingAmongClasses = SHARING_AMONG_CLASSES;
	
	
	// Semantic Interpreter
	semInterpreter = new SemanticInterpreter(tableMgr);
	if(!semInterpreter)
		return INTERNAL_ERR;
	
	// Logical plan generator
	logPlanGen = new LogPlanGen (tableMgr);
	if (!logPlanGen)
		return INTERNAL_ERR;
	
	return 0;
}

/**
 * Register a table (stream or a relation):
 *
 * The informatin about a table is encoded as a string.  This string is
 * first parsed using the parser.
 */

int ServerImpl::registerBaseTable(const char *tableInfo,
								  unsigned int tableInfoLen,
								  Interface::TableSource *input)
{
	int rc;
	NODE  *parseTree;
	unsigned int tableId;
	//printf("Table registered\n");
	// Tables can be registered only in S_APP_SPEC mode (before
	// endApplicationSpec() has been called)
	if (state != S_APP_SPEC)
		return INVALID_USE_ERR;	
	
	// Sanity check
	if (!tableInfo)
		return INVALID_PARAM_ERR;
	
	// Parse table info
	parseTree = Parser::parseCommand(tableInfo, tableInfoLen);
	if(!parseTree)
		return PARSE_ERR;
	
	// Store the parsed info with the table manager
	if(parseTree -> kind == N_REL_SPEC) {
		if ((rc = registerRelation (parseTree, tableMgr, tableId)) != 0)
			return rc;
	}
	else if(parseTree -> kind == N_STR_SPEC) {
		if ((rc = registerStream (parseTree, tableMgr, tableId)) != 0)
			return rc;
	}
	
	// Unknown command: parse error
	else {
		return PARSE_ERR;
	}
	
	// Inform the plan manager about the new table
	if ((rc = planMgr -> addBaseTable (tableId, input)) != 0)
		return rc;
	
	return 0;	
}

/**
 *  [[ To be implemented ]] 
 */ 
int ServerImpl::registerQuery (const char *querySpec, 
							   unsigned int querySpecLen,
							   Interface::QueryOutput *output,
							   unsigned int &queryId)
{	
	NODE *parseTree;
	Semantic::Query semQuery;
	Logical::Operator *logPlan;
	int rc;
	
	// This method can be called only in S_APP_SPEC state
	if (state != S_APP_SPEC){
		return INVALID_USE_ERR;
	}
	// Query String -> Parse Tree
	parseTree = Parser::parseCommand(querySpec, querySpecLen);
	if (!parseTree) {
		return PARSE_ERR;
	}
	
	// Parse Tree -> Semantic Query
	if((rc = semInterpreter -> interpretQuery (parseTree, semQuery)) != 0) {
		return rc;
	}

	/// Debug
#ifdef _DM_
	//printQuery (cout, semQuery, tableMgr);
#endif
	
	// At this point there are no errors in the query (syntactic or
	// semantic), so we register the query and get an internal id for it
	if ((rc = qryMgr -> registerQuery (querySpec, querySpecLen, queryId))
		!= 0) {
		return rc;
	}
	
	
	
	// SemanticQuery -> logical plan
	if ((rc = logPlanGen -> genLogPlan (semQuery, logPlan)) != 0) {
		return rc;
	}
	
#ifdef _DM_
	//cout << endl << endl << logPlan << endl;
#endif
	
	//Query Class Scheduling by Lory Al Moakar
	//copy current_class to the plan manager
	// so that when the phsical plan is generated the operators
	// are aware of the query class that these operators
	// belong to
	planMgr-> current_query_class = this->current_query_class;

	//end of Query Class Scheduling by LAM

	//query placement, by Thao Pham
	planMgr->b_active = semQuery.b_active;
	//end of query placement by Thao Pham

	printf("query registered\n");

	// Logical Plan -> Physical Plan (stored with plan manager)
	if ((rc = planMgr -> addLogicalQueryPlan (queryId, logPlan, output)) != 0){
		return rc;
	}
	
#ifdef _DM_
	//cout << endl << endl;
	//planMgr -> printPlan();
#endif
	
	return 0;
}

#ifdef _SYS_STR_
	
/**
 * Register a monitor query with the server.
 */

int ServerImpl::registerMonitor (const char *monitorSpec,
								 unsigned int monitorSpecLen,
								 Interface::QueryOutput *output,
								 unsigned int &monitorId)
{
	NODE *parseTree;
	Semantic::Query semQuery;
	Logical::Operator *logPlan;
	int rc;

	ASSERT (output);

	LOG << "Server: trying to interrupt exec ...";
	
	// Interrupt the server execution to register the monitor
	interruptExecution ();

	LOG << "done" << endl;
	
	// If we are not in the interrupted state, it means that
	// the server was not executing when we tried to interrupt
	if (state != S_INT) {
		LOG << "Register monitor when server not executing" << endl;		
		return 0;
	}

	LOG << "parsing..." << endl;
	
	// Query String -> Parse Tree
	parseTree = Parser::parseCommand(monitorSpec, monitorSpecLen);
	if (!parseTree) {
		resumeExecution ();
		return PARSE_ERR;
	}

	LOG << "Seminterp..." << endl;
	
	// Parse Tree -> Semantic Query
	if((rc = semInterpreter -> interpretQuery (parseTree, semQuery)) != 0) {
		resumeExecution ();
		return rc;
	}	

	LOG << "log plan..." << endl;
	
	// SemanticQuery -> logical plan
	if ((rc = logPlanGen -> genLogPlan (semQuery, logPlan)) != 0) {
		resumeExecution ();
		return rc;
	}

	LOG << "register query" << endl;
	
	// At this point there are no errors in the monitor query (syntactic or
	// semantic), so we register the query and get an internal id for it
	if ((rc = qryMgr -> registerQuery (monitorSpec, monitorSpecLen,
									   monitorId)) != 0) {
		resumeExecution ();
		return rc;
	}

	LOG << "add monitor" <<endl;
	
	if ((rc = planMgr -> addMonitorPlan (monitorId, logPlan,
										 output, scheduler)) != 0)
		return rc;

	LOG << "resume exec..." << endl;
	
	resumeExecution ();

	LOG << "done" << endl;
	
	return 0;
}

#endif


int ServerImpl::getQuerySchema (unsigned int queryId,
								char *schemaBuf,
								unsigned int schemaBufLen) {

#ifndef _SYS_STR_
	if (state != S_APP_SPEC)
		return INVALID_USE_ERR;
#endif
	
	return planMgr -> getQuerySchema (queryId, schemaBuf, schemaBufLen);
}

int ServerImpl::registerView(unsigned int queryId,
							 const char *tableInfo,
							 unsigned int tableInfoLen)
{
	int rc;
	NODE  *parseTree;
	unsigned int tableId;	
	
	// This method can be called only in S_APP_SPEC state
	if (state != S_APP_SPEC)
		return INVALID_USE_ERR;

	// Sanity check
	if (!tableInfo)
		return INVALID_PARAM_ERR;

	// Parse table info
	parseTree = Parser::parseCommand(tableInfo, tableInfoLen);
	if(!parseTree)
		return PARSE_ERR;
	
	// Store the parsed info with the table manager
	if(parseTree -> kind == N_REL_SPEC) {
		if ((rc = registerRelation (parseTree, tableMgr, tableId)) != 0)
			return rc;
	}
	
	else if(parseTree -> kind == N_STR_SPEC) {
		if ((rc = registerStream (parseTree, tableMgr, tableId)) != 0)
			return rc;
	}
	
	// Unknown command: parse error
	else {
		return PARSE_ERR;
	}
	
	// Indicate the mapping from query -> tableId to the plan manager
	if ((rc = planMgr -> map (queryId, tableId)) != 0)
		return rc;
	
	return 0;
}

/**
 * [[ to be implemented ]]
 */ 
int ServerImpl::endAppSpecification() 
{
	int rc;

	if (state != S_APP_SPEC)
		return INVALID_USE_ERR;
	
	if ((rc = planMgr -> optimize_plan ()) != 0)
		return rc;

	if ((rc = planMgr -> add_aux_structures ()) != 0)
		return rc;

	if ((rc = planMgr -> instantiate ()) != 0)
		return rc;
	
	//insert embedded shedders, by Thao Pham
	#ifdef _LOAD_MANAGE_
		if((rc=planMgr->insertDropOps())!=0)
			return rc;
	#endif
	
	//Lottery scheduling by Lory Al Moakar
	switch( SCHEDULING_POLICY ) {
	  
	case ROUND_ROBIN_SCHED:
	  scheduler = new Execution::RoundRobinScheduler();
	  break;

	case LOTTERY_SCHED:
	  scheduler = new Execution::LotteryScheduler();
	  break;
	  
	case LOTTERY_SCHED_ID:
	  scheduler = new Execution::LotterySchedulerID();
	  break;
//comment out by Thao Pham - ignore these schedulers for now

	/*case LOTTERY_SCHED_READY:
	  scheduler = new Execution::LotterySchedReady();
	  break;*/

	case HR_SCHEDULER:
	  scheduler = new Execution::HRScheduler();
	  ((Execution::HRScheduler * )scheduler)->STEADY_STATE = STEADY_STATE;
	  break;

	case QC_SCHEDULER:
	  scheduler = new Execution::QCScheduler(N_QUERY_CLASSES);
	  ((Execution::QCScheduler * )scheduler)->STEADY_STATE = STEADY_STATE;
	  ((Execution::QCScheduler * )scheduler)->quota_default = QUOTA;
	  ((Execution::QCScheduler * )scheduler)->honor_priorities = HONOR_PRIORITIES;

	  //Query Class Scheduling by Lory Al Moakar
	  // initialize the query class priorities to pass them  to the 
	  // scheduler 
	  scheduler ->n_query_classes = N_QUERY_CLASSES;
	  // pass the query priorities to the scheduler
	  for (int i = 0; i < N_QUERY_CLASSES; i++ ){
	    scheduler ->query_class_priorities[i] = QUERY_CLASS_PRIORITIES[i];
	    //printf( "SchedQuePriority %f ", QUERY_CLASS_PRIORITIES[i]);
	  }
	  //end of part 2 of Query Class Scheduling by LAM

	  break;
	 //weighted RR, by Thao Pham
	case WRR_SCHEDULER:
	  scheduler = new Execution::WRR_Scheduler(N_QUERY_CLASSES);
	  ((Execution::WRR_Scheduler * )scheduler)->quota_default = QUOTA;

	  //Query Class Scheduling by Lory Al Moakar
	  // initialize the query class priorities to pass them  to the 
	  // scheduler 
	  scheduler ->n_query_classes = N_QUERY_CLASSES;
	  // pass the query priorities to the scheduler
	  for (int i = 0; i < N_QUERY_CLASSES; i++ ){
	    scheduler ->query_class_priorities[i] = QUERY_CLASS_PRIORITIES[i];
	    printf( "SchedQuePriority %f ", QUERY_CLASS_PRIORITIES[i]);
	  }
	 
	 break;
	 //end of weighted RR 
	 
	  //commented by Thao Pham
	/*case REVERSE_RR:
	  scheduler = new Execution::RoundRobinScheduler();
	  ((Execution::RoundRobinScheduler * )scheduler) -> REVERSE_ORDER = true;
	  break;
	  
	  
	case QC_WITH_SRCS:
	  scheduler = new Execution::QCWithSrcs(N_QUERY_CLASSES);
	  ((Execution::QCWithSrcs * )scheduler)->STEADY_STATE = STEADY_STATE;
	  ((Execution::QCWithSrcs * )scheduler)->quota_default = QUOTA;
	  ((Execution::QCWithSrcs * )scheduler)->honor_priorities = HONOR_PRIORITIES;
	  //Query Class Scheduling by Lory Al Moakar
	  // initialize the query class priorities to pass them  to the 
	  // scheduler 
	  scheduler ->n_query_classes = N_QUERY_CLASSES;
	  // pass the query priorities to the scheduler
	  for (int i = 0; i < N_QUERY_CLASSES; i++ ){
	    scheduler ->query_class_priorities[i] = QUERY_CLASS_PRIORITIES[i];
	    //printf( "SchedQuePriority %f ", QUERY_CLASS_PRIORITIES[i]);
	  }
	  //end of part 2 of Query Class Scheduling by LAM

	  break; 
	  
	case WHR_SCHEDULER:
	  scheduler = new Execution::WHRScheduler();
	  ((Execution::WHRScheduler * )scheduler)->STEADY_STATE = STEADY_STATE;
	  //Query Class Scheduling by Lory Al Moakar
	  // initialize the query class priorities to pass them  to the 
	  // scheduler 
	  scheduler ->n_query_classes = N_QUERY_CLASSES;
	  // pass the query priorities to the scheduler
	  for (int i = 0; i < N_QUERY_CLASSES; i++ ){
	    scheduler ->query_class_priorities[i] = QUERY_CLASS_PRIORITIES[i];
	    //printf( "SchedQuePriority %f ", QUERY_CLASS_PRIORITIES[i]);
	  }
	    break;
	*/
//end of comment out by Thao Pham
//end of part 2 of Query Class Scheduling by LAM
	default:
	  return -1713;
   
	}
	fprintf(stdout,"time unit is %d", TIME_UNIT);
	
	//end of part 3 of Lottery scheduling by LAM
	
	// Response Time Calculation By Lory Al Moakar
	//initialize time_unit if specified by user
	scheduler -> time_unit = TIME_UNIT; 

	//end of part 2 of response time calculation by LAM
	
		
	//fprintf(stdout, "before initializing scheduler");
	if ((rc = planMgr -> initScheduler (scheduler)) != 0)
	  return rc;
	
	//load manager initialization, by Thao Pham
	
	for(int i=0; i< N_QUERY_CLASSES; i++)
	{
		//Execution::LoadManager    *loadMgr;
		Execution::LoadManager    *loadMgr = new Execution::LoadManager();
		
		loadMgr ->input_rate_time_unit = INPUT_RATE_TIME_UNIT;
		//loadMgr ->headroom_factor = HEADROOM_FACTOR;
		loadMgr->delay_tolerance = DELAY_TOLERANCE;
		loadMgr->initial_gap = INITIAL_GAP;
		loadMgr ->critical_above_factor = ((double)INITIAL_GAP)/100.0;
		loadMgr ->critical_below_factor = ((double)INITIAL_GAP)/100.0;
		
		loadMgr->ctrl_headroom = HEADROOM_FACTOR;
		
		loadMgr ->next_gap = NEXT_GAP;
		loadMgr ->load_manager_type = LOAD_MANAGER;
		loadMgr->headroom_factor = HEADROOM_FACTOR*((double)QUERY_CLASS_PRIORITIES[i])/10.0;
		
				
		char logfile[256];
		char str[] = {'_',(char)(i+49),0};
		strcpy(logfile, sheddingLogFile);
		strcat(logfile, str);
		
		//printf("\n%s\n",str);
		
		loadMgr->sheddingLogFile = fopen(logfile, "wt");  
		//end of part 1 of load manager initialization, by Thao Pham
		
		scheduler->addLoadManager(loadMgr);
	
	}

	if ((rc = planMgr -> initLoadManager(scheduler->loadMgrs)) != 0)
	  return rc;
	
	
	//end of load manager initialization, by Thao Pham
	//fprintf(stdout,"after scheduler is already initialized");
	state = S_PLAN_GEN;
	fprintf(stdout,"time unit is %d",scheduler -> time_unit );
	return 0;
}

/**
 * Get the plan for execution.  Should be called after endAppSpec()
 * method is called
 */
int ServerImpl::getXMLPlan (char *planBuf, unsigned int planBufLen)
{
#ifdef _SYS_STR_
	if (state != S_PLAN_GEN && state != S_EXEC)
		return INVALID_USE_ERR;
#else
	if (state != S_PLAN_GEN)
		return INVALID_USE_ERR;
#endif
	
	return planMgr -> getXMLPlan (planBuf, planBufLen);
}

/**
 * [[ To be implemented ]]
 */
int ServerImpl::beginExecution()
{
	int rc;

	if (state != S_PLAN_GEN)
		return INVALID_USE_ERR;
		
	// Transition from S_PLAN_GEN to S_EXEC (Critical code)
	pthread_mutex_lock (&mutex);

	// normal sequence
	if (state == S_PLAN_GEN) {
		state = S_EXEC;
	}

	// the execution has been interrupted already 
	else if (state == S_INT) {
		// wake up the interrupting thread
		pthread_cond_signal (&interruptThreadWait);
		
		// sleep until the interruption is over
		pthread_cond_wait (&mainThreadWait, &mutex);
		
		// Resume ..
		state = S_EXEC;
	}
	
	else if (state == S_END) {
		// do nothing
	}
	
	pthread_mutex_unlock (&mutex);	
	
	//by Thao Pham: load all data
	//if( (rc = planMgr->loadAllInputs())!=0)
	//	return rc;
	//end of all input loading	

	while (state == S_EXEC) {
		if ((rc = scheduler -> run (SCHEDULER_TIME)) != 0)
			return rc;
		
		ASSERT (state == S_EXEC || state == S_INT || state == S_END);
		pthread_mutex_lock (&mutex);
		
		// natural termination
		if (state == S_EXEC) {
			state = S_END;
		}
		
		// interrupted by interruptExecution()
		else if (state == S_INT) {			
			// wake up the interrupting thread
			pthread_cond_signal (&interruptThreadWait);
			
			// sleep until the interruption is over
			pthread_cond_wait (&mainThreadWait, &mutex);
			
			// Resume ..
			state = S_EXEC;
		}
		
		// terminated by stopExecution()
		else if (state == S_END) {
			// do nothing
		}
		
		pthread_mutex_unlock (&mutex);
	}
	
	ASSERT (state == S_END);
	
	if ((rc = planMgr -> printStat()) != 0)
		return rc;   
	
	return 0;
}

int ServerImpl::stopExecution ()
{
	
	ASSERT (state == S_PLAN_GEN || state == S_EXEC || state == S_END);
	pthread_mutex_lock (&mutex);
	if (state == S_EXEC) {	
		scheduler -> stop ();
	}
	state = S_END;
	pthread_mutex_unlock (&mutex);
	
	return 0;
}

int ServerImpl::interruptExecution ()
{
	ASSERT (state == S_PLAN_GEN || state == S_EXEC || state == S_END);
	
	pthread_mutex_lock (&mutex);
	
	if (state == S_EXEC) {		
		scheduler -> stop ();
		state = S_INT;
		stateWhenInterrupted = S_EXEC;
		
		// wait for the main scheduler thread to give you control
		pthread_cond_wait (&interruptThreadWait, &mutex);
	}
	
	else if (state == S_PLAN_GEN) {
		stateWhenInterrupted = S_PLAN_GEN;
		state = S_INT;
	}
	
	pthread_mutex_unlock (&mutex);
	return 0;
}

int ServerImpl::resumeExecution ()
{
	int rc;
	
	ASSERT (state == S_INT);
	
	pthread_mutex_lock (&mutex);
	
	// This does not actually run the scheduler, but just resets its state
	// so that the next run() call runs without returning immediately.
	if ((rc = scheduler -> resume ()) != 0)
		return rc;
	state = stateWhenInterrupted;

	ASSERT (state == S_EXEC || state == S_PLAN_GEN);
	
	pthread_cond_signal (&mainThreadWait);
	pthread_mutex_unlock (&mutex);
	
	return 0;
}

static int registerRelation (NODE *parseTree,
							 Metadata::TableManager *tableMgr,
							 unsigned int &relId)
{	
	int rc;
	
	const char   *relName;
	NODE         *attrList;	
	NODE         *attrSpec;
 	const char   *attrName;
	Type          attrType;
	unsigned int  attrLen;
	
	// Relation name
	relName = parseTree -> u.REL_SPEC.rel_name;
	ASSERT(relName);	
	
	if((rc = tableMgr -> registerRelation(relName, relId)) != 0)
		return rc;
	
	attrList = parseTree -> u.REL_SPEC.attr_list;
	ASSERT(attrList);
	
	// Process each attribute in the list
	while (attrList) {		
		ASSERT(attrList -> kind == N_LIST);		
		
		// Attribute specification
		attrSpec = attrList -> u.LIST.curr;
		
		ASSERT(attrSpec);
		ASSERT(attrSpec -> kind == N_ATTR_SPEC);
		
		attrName = attrSpec -> u.ATTR_SPEC.attr_name;
		attrType = attrSpec -> u.ATTR_SPEC.type;

		switch (attrType) {
		case INT:
			attrLen = INT_SIZE; break;			
		case FLOAT:
			attrLen = FLOAT_SIZE; break;
		case CHAR:
			attrLen = attrSpec -> u.ATTR_SPEC.len; break;			
		case BYTE:
			attrLen = BYTE_SIZE; break;
		default:
			ASSERT (0);
			break;
		}
		
		if((rc = tableMgr -> addTableAttribute(relId, attrName, 
											   attrType, attrLen)) != 0) {
			tableMgr -> unregisterTable (relId);
			return rc;
		}
		
		// next attribute
		attrList = attrList -> u.LIST.next;
	}
	
	return 0;
}

static int registerStream (NODE *parseTree,
						   Metadata::TableManager *tableMgr,
						   unsigned int &strId)
{
	int rc;
	
	const char   *strName;
	NODE         *attrList;	
	NODE         *attrSpec;
 	const char   *attrName;
	Type          attrType;
	unsigned int  attrLen;

	// Stream name
	strName = parseTree -> u.STR_SPEC.str_name;
	ASSERT(strName);
	
	if((rc = tableMgr -> registerStream(strName, strId)) != 0)
		return rc;
	
	attrList = parseTree -> u.STR_SPEC.attr_list;
	ASSERT(attrList);
	
	// Process each attribute in the list
	while (attrList) {
		
		ASSERT(attrList -> kind == N_LIST);
		
		// Attribute specification
		attrSpec = attrList -> u.LIST.curr;
		
		ASSERT(attrSpec);
		ASSERT(attrSpec -> kind == N_ATTR_SPEC);
		
		attrName = attrSpec -> u.ATTR_SPEC.attr_name;
		attrType = attrSpec -> u.ATTR_SPEC.type;
		
		switch (attrType) {
		case INT:
			attrLen = INT_SIZE; break;			
		case FLOAT:
			attrLen = FLOAT_SIZE; break;
		case CHAR:
			attrLen = attrSpec -> u.ATTR_SPEC.len; break;			
		case BYTE:
			attrLen = BYTE_SIZE; break;
		default:
			ASSERT (0);
			break;
		}
		
		if((rc = tableMgr -> addTableAttribute(strId, attrName, 
											   attrType, attrLen)) != 0) {
			tableMgr -> unregisterTable (strId);
			return rc;
		}
		
		// next attribute
		attrList = attrList -> u.LIST.next;
	}
	
	return 0;
}

Server *Server::newServer(std::ostream& LOG) 
{
	return new ServerImpl(LOG);
}

int ServerImpl::setConfigFile (const char *configFile)
{
	int rc;
	ConfigFileReader *configFileReader;
	ConfigFileReader::Param param;
	ConfigFileReader::ParamVal val;
	bool bValid;
	
	configFileReader = new ConfigFileReader (LOG);
	
	if ((rc = configFileReader -> setConfigFile (configFile)) != 0)
		return rc;
	
	//Query Class Scheduling by Lory Al Moakar
	// the index of the QUERY_CLASS_PRIORITIES array that we reached last
	int n_query_priorities = 0;
	//end of part 2 of Query Class Scheduling by LAM
	while (true) {
		
		if ((rc = configFileReader -> getNextParam (param, val, bValid))
			!= 0)
			return rc;
		
		if (!bValid)
			break;
		
		switch (param) {
		case ConfigFileReader::MEMORY_SIZE:
			MEMORY = (unsigned int)val.ival;
			break;
			
		case ConfigFileReader::QUEUE_SIZE:
			QUEUE_SIZE = (unsigned int)val.ival;
			break;
			
		case ConfigFileReader::SHARED_QUEUE_SIZE:
			SHARED_QUEUE_SIZE = (unsigned int)val.ival;
			break;
			
		case ConfigFileReader::INDEX_THRESHOLD:
			INDEX_THRESHOLD = val.dval;
			break;
			
		case ConfigFileReader::RUN_TIME:
			SCHEDULER_TIME = val.lval;
			break;

		case ConfigFileReader::CPU_SPEED:
			CPU_SPEED = (unsigned int)val.lval;
			break;
		// Response Time Calculation By Lory Al Moakar
		// read the time_unit as specified by the user
		// in the configuration file.
		case ConfigFileReader::TIME_UNIT:
		  TIME_UNIT = (unsigned int)val.ival;
		  break;
		//end of part 3 of response time calculation by LAM

		//Lottery scheduling by Lory Al Moakar
		case  ConfigFileReader::SCHEDULING_POLICY:
		  SCHEDULING_POLICY = (unsigned int)val.ival;
		  break;
		// end of part 4 of lottery scheduling by LAM  
	
		
		
		//Query Class Scheduling by Lory Al Moakar
		case ConfigFileReader::QUERY_CLASSES:
		  N_QUERY_CLASSES = val.ival;
		  LOG<< "Found "<<N_QUERY_CLASSES<<" query classes"<<endl;
		  break;
		case ConfigFileReader::CLASS_PRIORITY:
		  // if the user specified the number of classes < 
		  // the number of priorities specified 
		  // --> ignore the additional priorities
		  if ( n_query_priorities >= N_QUERY_CLASSES ) 
		    break;
		  // if we are still inputting priorities
		  QUERY_CLASS_PRIORITIES[n_query_priorities] = val.dval;
		  LOG<<"Priority of class "<< (n_query_priorities+1)<<" is ";
		  LOG << QUERY_CLASS_PRIORITIES[n_query_priorities] <<endl;
		  //increment n_query_priorities in order to record
		  // how many priorities inputted so far
		  ++n_query_priorities;

		  break;
		case  ConfigFileReader::STEADY_STATE:
		  STEADY_STATE = (unsigned int)val.ival;
		  break;
		  
		case  ConfigFileReader::QUOTA:
		  QUOTA = (unsigned int)val.ival;
		  break;
		case  ConfigFileReader::HONOR_PRIORITIES:
		  HONOR_PRIORITIES = (unsigned int)val.ival;
		  break;

		case ConfigFileReader::SHARING_AMONG_CLASSES:
		  SHARING_AMONG_CLASSES = (unsigned int)val.ival;
		  break;
		//end of part 3 of Query Class Scheduling by LAM


		//load managing, by Thao Pham
		
		case ConfigFileReader::INPUT_RATE_TIME_UNIT:
			INPUT_RATE_TIME_UNIT = (unsigned int)val.ival;
			break;
			
		case ConfigFileReader::HEADROOM_FACTOR:
			HEADROOM_FACTOR = val.dval;
			break;
		
		case ConfigFileReader::DELAY_TOLERANCE:
			DELAY_TOLERANCE = val.dval;
			break;
		case ConfigFileReader::INITIAL_GAP:
			INITIAL_GAP = val.ival;
			break;
		case ConfigFileReader::NEXT_GAP:
			NEXT_GAP = val.ival;
			break;
		case ConfigFileReader::LOAD_MANAGER:
			LOAD_MANAGER = val.ival;
			break;
			
		default:
			break;
		}
	}

	delete configFileReader;
	
	return 0;
}

//Load shedding by Thao Pham: add shedding log file info
int ServerImpl::setSheddingLogFile(const char *sheddingLogFile)
{
	this->sheddingLogFile =sheddingLogFile; 
	return 0;
}

