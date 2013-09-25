#ifndef _COMPAREOPS_
#define _COMPAREOPS_

#ifndef _OPERATOR_
#include "execution/operators/operator.h"
#endif



/**
 * @file compareOPS.h
 * @date Feb 9, 2009
 * @brief this is a class that is used by shared operators to compare 
 * the operators.
*/
namespace Execution {
  class CompareOPS {
  public:
    
    bool operator() ( const Operator*  op1  , const Operator* op2 ) const  {
      if ( op1->priority >= op2->priority)
	return false;
      return true;
    }
  };
};

#endif
