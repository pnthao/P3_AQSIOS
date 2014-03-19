#ifndef _PARAMS_
#define _PARAMS_

/// Size of the memory managed by MemoryManager that is available to the
/// execution units  
unsigned int MEMORY;

/// Default memory size = 64 MB
static const unsigned int MEMORY_DEFAULT = (1 << 20) * 64;

/// Memory allocated to a queue in number of pages
unsigned int QUEUE_SIZE;

/// Default queue size = 1 Page
static const unsigned int QUEUE_SIZE_DEFAULT = 1;

/// Memory allocated to a shared queue in number of pages
unsigned int SHARED_QUEUE_SIZE;

/// Default shared queue size = 30 Page
static const unsigned int SHARED_QUEUE_SIZE_DEFAULT = 30;

/// Threshold used for index bucket splitting
double INDEX_THRESHOLD;

static const double INDEX_THRESHOLD_DEFAULT = 0.85;

/// Number of iterations of the scheduler
long long int SCHEDULER_TIME;

static const long long int SCHEDULER_TIME_DEFAULT = 5000000 * 100;

/// The cpu speed MHz
unsigned int CPU_SPEED;

static const int CPU_SPEED_DEFAULT = 2000; // 2000 MHz

// Response Time Calculation By Lory Al Moakar
// a variable used to specify the length of a time unit in nanosec
//used when admitting tuples and when calculating their response time.
int TIME_UNIT;

//the default value for the TIME_UNIT parameter.
static const int TIME_UNIT_DEFAULT = 1; //1 ns
//end of response time calculation by LAM

//load managing params , by Thao Pham

//length in nanosec of time unit for input rate (input rate is in terms of num. of tuples/timeunit
unsigned int INPUT_RATE_TIME_UNIT;
static const unsigned int INPUT_RATE_TIME_UNIT_DEFAULT =1000000000;//1 sec

//headroom factor
double HEADROOM_FACTOR;
static const double HEADROOM_FACTOR_DEFAULT = 0.5;

int INITIAL_GAP;
static const int INITIAL_GAP_DEFAULT = 10;

int NEXT_GAP;
static const int NEXT_GAP_DEFAULT = 5;

int LOAD_MANAGER;
static const int LOAD_MANAGER_DEFAULT = 1; //ALM


/*delay tolerance, for which the system keeps the response time of an output to be
 * delay_tolerance times its average processing time
 */
double DELAY_TOLERANCE;
static const double DELAY_TOLERANCE_DEFAULT = 5;
//end of load managing params, by Thao Pham

//Lottery scheduling by Lory Al Moakar
//the scheduling policy that will be used by the scheduler
// to schedule operators during run time
int SCHEDULING_POLICY;

//the default scheduling policy is round_robin
// whose value is 1
static const int ROUND_ROBIN_SCHED = 1;

// the value for the lottery scheduler is 2
static const int LOTTERY_SCHED = 2;

// the value for the lottery schedulerID is 3
static const int LOTTERY_SCHED_ID = 3;

//end of lottery scheduling by LAM

//the value for the lottery scheduler with ready flags
static const int LOTTERY_SCHED_READY = 4;

//the value for the HR scheduler is 5 
static const int HR_SCHEDULER  = 5;

//the value for Query class scheduler is 6
static const int QC_SCHEDULER = 6;

//the value for the reversed round robin is 7
static const int REVERSE_RR   = 7;

//the value for the QC scheduler with srcs on top level is 8
static const int QC_WITH_SRCS = 8;

//the value for the weighted HR scheduler is 9
static const int WHR_SCHEDULER = 9;

//the value for weighted RR scheduler is 10, added by Thao Pham
static const int WRR_SCHEDULER = 10;


//end of lottery scheduling by LAM


//Query Class Scheduling by Lory Al Moakar

// the number of query classes inputted by the user
int N_QUERY_CLASSES;

// the default number for N_QUERY_CLASSES
static const int N_QUERY_CLASSES_DEFAULT = 1;

// the maximum number of query classes in the system
static const int MAX_N_QUERY_CLASSES = 10 ;

// an array that holds the priorities of the query classes
// as inputted by the user
double QUERY_CLASS_PRIORITIES [MAX_N_QUERY_CLASSES]; 

//the default priority for any query class
static const double DEFAULT_QC_PRIORITY = 1;

// are we honoring the user priorities as is ?
bool HONOR_PRIORITIES;

//the default value for honor_priorities is true
static const bool HONOR_PRIORITIES_DEFAULT = true;

// the number of operator refreshes that the system needs
//to reach steady state --> the number of times the operators
// need to refresh their priorities to reach steady state
int STEADY_STATE;
//the default value for the steady_state variable is 0 
// i.e. the system starts in steady state
static const int STEADY_STATE_DEFAULT = 0;

// the number of time units to run each QCHR scheduler for
//this is used by the QC_scheduler
int QUOTA;
// The default value for quota is 1 i.e. each scheduler
// has 1 timeunit to execute for 
static const int QUOTA_DEFAULT = 1;

//added in order to signify if there is sharing of regular ops
// ( non- source ops ) among query classes or not
bool SHARING_AMONG_CLASSES;
//the default value for sharing among classes is true 
// i.e. if the user does not specify , we assume that the system 
// should operate in best form --> sharing whenever possible
static const bool SHARING_AMONG_CLASSES_D = true;

//end of Query Class Scheduling by LAM 


#endif
