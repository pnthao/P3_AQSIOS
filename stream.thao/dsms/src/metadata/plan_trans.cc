/**
 * @file    plan_trans.cc
 * @date    Aug. 24, 2004
 * @brief   Routines to transform plans into (hopefully) better plans.
 */

#include <stdio.h>
#ifndef _PLAN_MGR_IMPL_
#include "metadata/plan_mgr_impl.h"
#endif

#ifndef _DEGUG_
#include "common/debug.h"
#endif

//Operator Sharing By Lory Al Moakar
#ifndef _CPP_VECTOR_
#include <vector>
#endif
//end of part 1 of operator sharing by LAM

using namespace Metadata;
using namespace Physical;
using namespace std;

/**
 * Remove this operator from a plan, and connect all its inputs to its
 * outputs directly.
 */

static int shortCircuitInputOutput (Operator *op);
static int addOutput (Operator *child, Operator *parent);
static bool operator == (Attr a1, Attr a2);

//Operator Sharing By Lory Al Moakar
static bool operator == (Expr e1, Expr e2);

static bool operator == (BExpr b1, BExpr b2);

/**
 * this method compares the specific properties of a select operator
 * @return true if they are the same  false otherwise
 * @param Operator* op1 the first select operator
 * @param Operator* op2 the second select operator to compare
 */
static bool compareSelects ( Operator *op1, Operator *op2 );
/**
 * this method compares the specific properties of a project operator
 * @return true if they are the same  false otherwise
 * @param Operator* op1 the first project operator
 * @param Operator* op2 the second project operator to compare
 */
static bool compareProjects ( Operator *op1, Operator *op2 );

/**
 * this method compares the specific properties of a range window operator
 * @return true if they are the same  false otherwise
 * @param Operator* op1 the first range win operator
 * @param Operator* op2 the second range win operator to compare
 */
static bool compareRangeWins ( Operator *op1, Operator *op2 );
/**
 * this method compares the specific properties of a range window operator
 * @return true if they are the same  false otherwise
 * @param Operator* op1 the first row window operator
 * @param Operator* op2 the second row window operator to compare
 */
static bool compareRowWins ( Operator *op1, Operator *op2 );
/**
 * this method compares the specific properties of a group aggr operator
 * @return true if they are the same  false otherwise
 * @param Operator* op1 the first group aggr operator
 * @param Operator* op2 the second group aggr operator to compare
 */
static bool compareGroupAggrs ( Operator *op1, Operator *op2 );
/**
 * this method compares the specific properties of a Partition window operator
 * @return true if they are the same  false otherwise
 * @param Operator* op1 the first  Partition window operator
 * @param Operator* op2 the second Partition window operator to compare
 */
static bool comparePartnWins ( Operator *op1, Operator *op2 );
/**
 * this method compares the specific properties of a join operator
 * this method should only be called after the program has checked the 
 * inputs to the join
 * this method does NOT compare the inputs to the join
 * @return true if they are the same  false otherwise
 * @param Operator* op1 the first  join operator
 * @param Operator* op2 the second join operator to compare
 */
static bool compareJoins ( Operator *op1, Operator *op2 );
/**
 * this method compares the specific properties of a join project operator
 * this method should only be called after the program has checked the 
 * inputs to the join
 * this method does NOT compare the inputs to the join
 * @return true if they are the same  false otherwise
 * @param Operator* op1 the first  join project operator
 * @param Operator* op2 the second join project operator to compare
 */
static bool compareJoinProjs ( Operator *op1, Operator *op2 );

/**
 * This method compares the general characteristics of two operators 
 * It calls the above methods when all the general characteristics 
 * are equal.
 * @return true if they are the same  false otherwise
 * @param Operator* op1 the first operator 
 * @param Operator* op2 the second operator to compare 
 * 
 */

static bool compareOperators( Operator *op1, Operator *op2 );

/**
 * This method combines both operators into one operator.
 * @return true if the merge was successful false otherwise
 * @param Operator* op1 the first operator, this reference 
 * the resulting operator.
 * @param Operator* op2 the second operator 
 * 
 */

static bool mergeOperators ( Operator *op1, Operator *op2, bool sharing_among_class);

//end of Part 2 of Operator Sharing by LAM 






int PlanManagerImpl::removeQuerySources ()
{
	int rc;
	Operator *op, *nextOp;
	
	// Iterate through all the used ops and remove query sources
	op = usedOps;
	while (op) {
		
		if (op -> kind == PO_QUERY_SOURCE) {
			rc = shortCircuitInputOutput (op);
			
			if (rc != 0)
				return rc;
			
			// we remember the next used op before freeing op
			nextOp = op -> next;
			free_op (op);
			
			op = nextOp;
		}
		
		else {
			op = op -> next;
		}
	}
	
	return 0;		
}

int PlanManagerImpl::addSinks ()
{
	int rc;
	
	Operator *op;
	Operator *sink;
	
	op = usedOps;
	while (op) {
		if ((op -> numOutputs == 0) && (op -> kind != PO_OUTPUT) &&
			(op -> kind != PO_SS_GEN)) {
			// create a new sink operator to read off the output of op
			if ((rc = mk_sink (op, sink)) != 0)
				return rc;
		}
		
		op = op -> next;
	}

	return 0;
}

static void append (BExpr *&dest, BExpr *src)
{
	BExpr *p;
	
	// destination predicate list empty
	if (!dest) {
		dest = src;
		return;
	}

	p = dest;
	while (p -> next)
		p = p -> next;	
	p -> next = src;
	
	return;
}

/**
 * Transform an attribute in a operator above join to an equivalent 
 * attribute within the join operator.
 */
static void transToJoinAttr (Attr &attr, Operator *join)
{
	ASSERT (join);
	ASSERT (attr.input == 0);
	ASSERT (attr.pos < join -> numAttrs);
	
	// We only deal with binary joins
	ASSERT (join -> numInputs == 2);
	ASSERT (join -> inputs [0]);
	ASSERT (join -> inputs [1]);
	ASSERT (join -> kind == PO_JOIN ||
			join -> kind == PO_STR_JOIN);

	if (join -> kind == PO_JOIN) {
		
		if (join -> u.JOIN.numOuterAttrs <= attr.pos) {
			attr.input = 1;
			attr.pos -= join -> u.JOIN.numOuterAttrs;
		}
		
	}
	
	else {
		
		if (join -> u.STR_JOIN.numOuterAttrs <= attr.pos) {
			attr.input = 1;
			attr.pos -= join -> u.STR_JOIN.numOuterAttrs;
		}
		
	}
	
}

/**
 * Transform an arithmetic expression in an operator above a join to an 
 * equiv. expression within the join.  The transformation is "in-place". 
 *
 * @param expr cannot be null.
 */

static void transToJoinExpr (Expr *expr, Operator *join)
{
	ASSERT (expr);
	
	switch (expr -> kind) {
	case CONST_VAL: break; // do nothing
	case COMP_EXPR:
		transToJoinExpr (expr -> u.COMP_EXPR.left, join);
		transToJoinExpr (expr -> u.COMP_EXPR.right, join);
		break;
	case ATTR_REF:
		transToJoinAttr (expr -> u.attr, join);
		break;
	default:
		break;
	}
}

/**
 * Transform a predicate in an operator above a join to an
 * equiv. predicate within the join. The transformation is "in-place".
 *
 * @param pred   predicate to be transformed. Could be null
 */

static void transToJoinPred (BExpr *pred, Operator *join)
{
	if (pred) {
		ASSERT (pred -> left);
		ASSERT (pred -> right);
		
		transToJoinExpr (pred -> left, join);
		transToJoinExpr (pred -> right, join);
		
		// pred -> next == 0 handled properly
		transToJoinPred (pred -> next, join);
	} 
}

static bool mergeSelect (Operator *op)
{
	Operator *inOp;
	
	ASSERT (op -> numInputs == 1);
	ASSERT (op -> inputs [0]);
	ASSERT (op -> inputs [0] -> numOutputs >= 1);
	
	inOp = op -> inputs [0];

	// We cannot merge if someone else is reading from the child operator
	if (inOp -> numOutputs != 1)
		return false;
	
	if (inOp -> kind == PO_SELECT) {
		append (inOp -> u.SELECT.pred, op -> u.SELECT.pred);
		return true;		
	}
	
	if (inOp -> kind == PO_JOIN) {		
		// Transform the predicate in the select to an equivalent
		// predicate in the join.
		transToJoinPred (op -> u.SELECT.pred, inOp);
		
		append (inOp -> u.JOIN.pred, op -> u.SELECT.pred);
		return true;
	}
	
	if (inOp -> kind == PO_STR_JOIN) {
		// Transform the predicate in the select to an equivalent
		// predicate in the join.
		transToJoinPred (op -> u.SELECT.pred, inOp);
		
		append (inOp -> u.STR_JOIN.pred, op -> u.SELECT.pred);
		return true;
	}
	
	return false;
}

int PlanManagerImpl::mergeSelects ()
{
	int rc;
	Operator *op, *nextOp;
	
	// Iterate through all the active ops and try merging selects
	op = usedOps;
	while (op) {
		
		// If we manage to successfully merge a select with its child
		// (another select or a join), we short circuit it
		if (op -> kind == PO_SELECT && mergeSelect (op)) {
			
			rc = shortCircuitInputOutput (op);
			if (rc != 0)
				return rc;
			
			nextOp = op -> next;
			free_op (op);
			
			op = nextOp;			
		}
		
		else {
			op = op -> next;
		}
	}
	
	return 0;	
}

static bool mergeProject (Operator *project)
{
	Operator *inOp;
	BExpr *pred;
	
	ASSERT (project);
	ASSERT (project -> numInputs == 1);
	ASSERT (project -> inputs [0]);   
	
	inOp = project -> inputs [0];

	// We cannot merge if someone is reading from the child operator.
	if (inOp -> numOutputs > 1)
		return false;
	
	if (inOp -> kind == PO_JOIN) {

		pred = inOp -> u.JOIN.pred;
		
		// Transform and copy the projections ...
		for (unsigned int a = 0 ;a < project -> numAttrs ; a++) {
			transToJoinExpr (project -> u.PROJECT.projs [a], inOp);
		}
		
		// transform the input operator to a join-project
		inOp -> kind = PO_JOIN_PROJECT;

		// The output schema of the new operator is the same as the project
		inOp -> numAttrs = project -> numAttrs;
		for (unsigned int a = 0 ; a < inOp -> numAttrs ; a++) {
			inOp -> attrTypes [a] = project -> attrTypes [a];
			inOp -> attrLen [a] = project -> attrLen [a];
			inOp -> u.JOIN_PROJECT.projs [a] =
				project -> u.PROJECT.projs [a];
		}
		
		ASSERT (inOp -> bStream == project -> bStream);
		ASSERT (inOp -> outputs [0] == project);
		
		inOp -> u.JOIN_PROJECT.pred = pred;				
		
		return true;
	}

	if (inOp -> kind == PO_STR_JOIN) {
		
		pred = inOp -> u.STR_JOIN.pred;
		
		// Transform and copy the projections
		for (unsigned int a = 0 ; a < project -> numAttrs ; a++) {
			transToJoinExpr (project -> u.PROJECT.projs [a], inOp);
		}			
		
		// transform the input to the str-join-project
		inOp -> kind = PO_STR_JOIN_PROJECT;

		// The output schema of the new op is the same as the project
		inOp -> numAttrs = project -> numAttrs;
		for (unsigned int a = 0 ; a < inOp -> numAttrs ; a++) {
			inOp -> attrTypes [a] = project -> attrTypes [a];
			inOp -> attrLen [a] = project -> attrLen [a];
			inOp -> u.STR_JOIN_PROJECT.projs [a] =
				project -> u.PROJECT.projs [a];
		}
		
		ASSERT (inOp -> bStream == project -> bStream);
		ASSERT (inOp -> outputs [0] == project);

		inOp -> u.STR_JOIN_PROJECT.pred = pred;		
		
		return true;
	}
	
	return false;
}

int PlanManagerImpl::mergeProjectsToJoin()
{
	int rc;
	Operator *op, *nextOp;

	// Iterate through all the active ops and try merging projects
	op = usedOps;
	
	while (op) {
		
		if (op -> kind == PO_PROJECT && mergeProject (op)) {
			
			rc = shortCircuitInputOutput (op);
			if (rc != 0)
				return rc;
			
			nextOp = op -> next;
			free_op(op);
			
			op = nextOp;
		}
		
		else {
			op = op -> next;
		}
	}
	
	return 0;
}

static bool existsCount (Operator *op)
{
	unsigned int numAggrAttrs;

	numAggrAttrs = op -> u.GROUP_AGGR.numAggrAttrs;

	for (unsigned int a = 0 ; a < numAggrAttrs ; a++)
		if (op -> u.GROUP_AGGR.fn [a] == COUNT)
			return true;
	
	return false;
}

static bool existsSum (Operator *op, Attr attr)
{
	unsigned int numAggrAttrs;

	numAggrAttrs = op -> u.GROUP_AGGR.numAggrAttrs;

	for (unsigned int a = 0 ; a < numAggrAttrs ; a++)
		if ((op -> u.GROUP_AGGR.aggrAttrs [a] == attr) &&
			(op -> u.GROUP_AGGR.fn [a] == SUM))
			return true;

	return false;
}

static int addCount (Operator *op)
{
	unsigned int numAggrAttrs;
	
	numAggrAttrs = op -> u.GROUP_AGGR.numAggrAttrs;
	
	if (numAggrAttrs >= MAX_AGGR_ATTRS)
		return -1;
	
	op -> u.GROUP_AGGR.aggrAttrs [numAggrAttrs].input = 0;
	op -> u.GROUP_AGGR.aggrAttrs [numAggrAttrs].pos = 0;
	op -> u.GROUP_AGGR.fn [numAggrAttrs] = COUNT;
	op -> u.GROUP_AGGR.numAggrAttrs ++;

	if (op -> numAttrs >= MAX_ATTRS)
		return -1;
	
	op -> attrTypes [ op -> numAttrs ] = INT;
	op -> attrLen [ op -> numAttrs ] = INT_SIZE;
	op -> numAttrs ++;
	
	return 0;
}

static int addSum (Operator *op, Attr attr)
{
	unsigned int numAggrAttrs;
	
	numAggrAttrs = op -> u.GROUP_AGGR.numAggrAttrs;
	
	if (numAggrAttrs >= MAX_AGGR_ATTRS)
		return -1;

	op -> u.GROUP_AGGR.aggrAttrs [numAggrAttrs] = attr;
	op -> u.GROUP_AGGR.fn [numAggrAttrs] = SUM;
	op -> u.GROUP_AGGR.numAggrAttrs ++;

	if (op -> numAttrs >= MAX_ATTRS)
		return -1;
	
	op -> attrTypes [ op -> numAttrs ] =
		op -> inputs [0] -> attrTypes [attr.pos];
	op -> attrLen [ op -> numAttrs ] =
		op -> inputs [0] -> attrLen [attr.pos];
	op -> numAttrs ++;
	
	return 0;
}

static int addIntAggrsGby (Operator *op)
{
	int rc;
	unsigned int numAggrAttrs;

	numAggrAttrs = op -> u.GROUP_AGGR.numAggrAttrs;
	
	ASSERT (op -> inputs [0]);
	
	// If the input to a gby aggr. operator is not a stream we need a
	// maintain a count aggr. within the operator to determine when a
	// group no longer exists.
	if (!op -> inputs [0] -> bStream && !existsCount (op)) { 
		rc = addCount (op);
		if (rc != 0) return rc;
	}
	
	for (unsigned int a = 0 ; a < numAggrAttrs ; a++) {
		if (op -> u.GROUP_AGGR.fn [a] != AVG)
			continue;
		
		if (!existsCount (op)) {
			rc = addCount (op);
			if (rc != 0) return rc;
		}
		
		if (!existsSum (op, op -> u.GROUP_AGGR.aggrAttrs [a])) {
			rc = addSum (op, op -> u.GROUP_AGGR.aggrAttrs [a]);
			if (rc != 0) return rc;
		}
	}
	
	return 0;	
}

static int addIntAggrsDistinct (Operator *op)
{
	int rc;
	
	ASSERT (op -> inputs [0]);
	
	if (!op -> inputs [0] -> bStream) {
		rc = addCount (op);
		if (rc != 0) return rc;
	}
	
	return 0;
}

int PlanManagerImpl::addIntAggrs ()
{
	int rc;
	Operator *op;

	// Iterate through all the active ops
	op = usedOps;
	while (op) {

		if (op -> kind == PO_GROUP_AGGR) {
			if ((rc = addIntAggrsGby (op)) != 0) {
				return rc;
			}
		}
		
		else if (op -> kind == PO_DISTINCT) {
			if ((rc = addIntAggrsDistinct (op)) != 0) {
				return rc;
			}
		}
		
		op = op -> next;		
	}
	
	return 0;
}

int PlanManagerImpl::mergeSelects_mon (Operator *&plan,
									   Operator **opList,
									   unsigned int &numOps)
{
	int rc;
	Operator *op;
	
	for (unsigned int o = 0 ; o < numOps ; ) {
		op = opList [o];
		
		// If we manage to successfully merge a select with its child
		// (another select or a join), we short circuit it
		if (op -> kind == PO_SELECT && mergeSelect (op)) {

			// Root operator is never select, it is output
			ASSERT (op != plan);
			
			rc = shortCircuitInputOutput (op);
			if (rc != 0)
				return rc;
			
			// Move the last operator to the current position
			numOps--;
			opList [o] = opList [numOps];
			
			free_op (op);
		}
		
		else {
			o++;
		}
	}

	return 0;
}

int PlanManagerImpl::mergeProjectsToJoin_mon (Operator *&plan,
											  Operator **opList,
											  unsigned int &numOps)
{
	int rc;
	Operator *op;
	
	for (unsigned int o = 0 ; o < numOps ; ) {
		op = opList [o];
		
		// If we manage to successfully merge a project with its child
		// (a join), we short circuit it
		if (op -> kind == PO_PROJECT && mergeProject (op)) {
			
			// Root operator is never project, it is output
			ASSERT (op != plan);
			
			rc = shortCircuitInputOutput (op);
			if (rc != 0)
				return rc;
			
			// Move the last operator to the current position
			numOps--;
			opList [o] = opList [numOps];
			
			free_op (op);
		}
		
		else {
			o++;
		}
	}
	
	return 0;	
}

int PlanManagerImpl::addIntAggrs_mon (Operator *&plan,
									  Operator **opList,
									  unsigned int &numOps)
{
	int rc;
	Operator *op;

	for (unsigned int o = 0 ; o < numOps ; o++) {
		op = opList [o];
			
		if (op -> kind == PO_GROUP_AGGR) {
			if ((rc = addIntAggrsGby (op)) != 0) {
				return rc;
			}
		}
		
		else if (op -> kind == PO_DISTINCT) {
			if ((rc = addIntAggrsDistinct (op)) != 0) {
				return rc;
			}
		}		
	}
	
	return 0;
}

static unsigned int getOutputIndex (Operator *child, Operator *parent)
{
	ASSERT (child);
	ASSERT (parent);
	
	for (unsigned int o = 0 ; o < child -> numOutputs ; o++)
		if (child -> outputs [o] == parent)
			return o;
	
	// should never come here
	ASSERT (0);
	return 0;
}

static unsigned int getInputIndex (Operator *parent, Operator *child)
{
	ASSERT (child);
	ASSERT (parent);
	
	for (unsigned int i = 0 ; i < parent -> numInputs ; i++)
		if (parent -> inputs [i] == child)
			return i;
	
	// should never come here
	ASSERT (0);
	return 0;
}

/**
 * Remove this operator from a plan, and directly connect its input to all
 * its outputs.
 */

static int shortCircuitInputOutput (Operator *op)
{
	int rc;
	Operator *inOp;
	unsigned int inputIdx;
	unsigned int outputIdx;
	
	ASSERT (op);
	ASSERT (op -> numInputs == 1);
	ASSERT (op -> inputs [0]);
	ASSERT (op -> inputs [0] -> numOutputs >= 1);
	
	// input operator
	inOp = op -> inputs [0];
	
	// position of op among inOp's outputs
	outputIdx = getOutputIndex (inOp, op);
	
	// update all the output operators
	for (unsigned int o = 0 ; o < op -> numOutputs ; o++) {
		
		// position of op among output operators inputs
		inputIdx = getInputIndex (op -> outputs [o], op);
		
		// short-circuit
		op -> outputs [o] -> inputs [inputIdx] = inOp;
		
		if ((rc = addOutput (inOp, op -> outputs [o])) != 0)
			return rc;						
	}
	
	// move the last output of inOp to outputIdx position [This works even
	// if op had no outputs - if you can't understand this never mind :) ]
	inOp -> outputs [outputIdx] =
		inOp -> outputs [-- (inOp -> numOutputs) ];
	
	return 0;
}

static int addOutput (Operator *child, Operator *parent)
{
	ASSERT (child);

	if (child -> numOutputs >= MAX_OUT_BRANCHING) {
		return -1;
	}

	child -> outputs [child -> numOutputs ++] = parent;
	return 0;
}

static bool operator == (Attr a1, Attr a2)
{
	return ((a1.input == a2.input) && (a1.pos == a2.pos));
}

//------------------------------------------------------
//Operator Sharing By Lory Al Moakar

static bool operator == ( Expr e1, Expr e2 ) {
  if ( e1.kind != e2.kind )
    return false;
  switch ( e1.kind ) {
  case CONST_VAL: 
    if ( e1.type != e2.type ) 
      return false;
    switch ( e1.type ) 
      {
      case INT:
	if ( e1.u.ival != e2.u.ival)
	  return false;
	return true;
	
      case FLOAT:
	if ( e1.u.fval != e2.u.fval)
	  return false;
	return true;
      case BYTE:
	if ( e1.u.bval != e2.u.bval)
	  return false;
	return true;
      case CHAR:
	if ( strcmp (e1.u.sval, e2.u.sval)!= 0)
	  return false;
	return true;	
      default:
	printf("unrecognized expr");
	return false;
	
      }
  case COMP_EXPR:
    if ( e1.u.COMP_EXPR.op != e2.u.COMP_EXPR.op)
      return false;
    if ( !( *(e1.u.COMP_EXPR.left) == *(e2.u.COMP_EXPR.left)))
      return false;
    if ( !( *(e1.u.COMP_EXPR.right) == *(e2.u.COMP_EXPR.right)))
      return false;   

    return true;
  
  
 case ATTR_REF:
   if ( !(e1.u.attr == e2.u.attr) )
     return false;
   return true;
   
  default:
    printf("Unknown Expression");
    return false;
  }
  return false;

}

static bool operator ==( BExpr b1, BExpr b2) {
  if ( b1.op != b2.op )
    return false;
  if ( ! ( *(b1.left) == *(b2.left) ))
    return false;
  if ( ! ( *(b1.right) == *(b2.right) ))
    return false;
  if ( b1.next && b2.next ){
    if ( !(*(b1.next) == *(b2.next)) )
      return false;  
  }
  else if (( !b1.next && b2.next ) || ( b1.next && !b2.next ) )
    return false;
  
  return true;

}

static bool compareSelects ( Operator *op1, Operator *op2 ){
  //printf ( "comparing selects: %d %d \n", op1->id, op2->id);
  //bool dbg = (*(op1->u.SELECT.pred) == *(op2->u.SELECT.pred));
  //if (dbg)
  //  printf ( "EQUAL\n");
  return (*(op1->u.SELECT.pred) == *(op2->u.SELECT.pred));

}


static bool compareProjects ( Operator *op1, Operator *op2 ){
  int i = 0;
  for ( i = 0; i < op1->numAttrs; i ++ )
    if ( !(*(op1->u.PROJECT.projs[i]) == *(op2->u.PROJECT.projs[i])))
      return false;

  return true;
  
}

static bool compareRangeWins ( Operator *op1, Operator *op2 ){
  //printf ( "comparing rangewins: %d %d \n", op1->id, op2->id);
  //bool dbg = ( op1->u.RANGE_WIN.timeUnits == op2->u.RANGE_WIN.timeUnits);
  //if (dbg)
  //  printf ( "EQUAL\n");

  return ( op1->u.RANGE_WIN.timeUnits == op2->u.RANGE_WIN.timeUnits);
  
}

static bool compareRowWins ( Operator *op1, Operator *op2 ){
  return ( op1->u.ROW_WIN.numRows == op2->u.ROW_WIN.numRows);
  
}

static bool compareGroupAggrs ( Operator *op1, Operator *op2 ){
  //first compare the numbers
  if ( op1->u.GROUP_AGGR.numGroupAttrs != op2->u.GROUP_AGGR.numGroupAttrs)
    return false;
  if ( op1->u.GROUP_AGGR.numAggrAttrs != op2->u.GROUP_AGGR.numAggrAttrs)
    return false;
  int i = 0;
  //compare the aggr attributes and the aggr functions
  for ( i =0; i < op1->u.GROUP_AGGR.numAggrAttrs; i++ ){
    if ( !(op1->u.GROUP_AGGR.aggrAttrs[i] == op2->u.GROUP_AGGR.aggrAttrs[i]) )
      return false;
    if ( op1->u.GROUP_AGGR.fn[i] != op2->u.GROUP_AGGR.fn[i])
      return false;
  }
  
  //compare the group by attrs
  for ( i = 0; i < op1->u.GROUP_AGGR.numGroupAttrs; i++)
    if ( !(op1->u.GROUP_AGGR.groupAttrs[i] == op2->u.GROUP_AGGR.groupAttrs[i]))
      return false;

  //return true since everything seems to be the same
  return true;

}

static bool comparePartnWins ( Operator *op1, Operator *op2 ){
  //compare the numbers
  if ( op1->u.PARTN_WIN.numPartnAttrs != op2 ->u.PARTN_WIN.numPartnAttrs)
    return false;
  if ( op1->u.PARTN_WIN.numRows !=  op2->u.PARTN_WIN.numRows)
    return false;

  //compare the partition attrs
  int i = 0;
  for ( i =0; i < op1->u.PARTN_WIN.numPartnAttrs; i++ )
    if ( !(op1->u.PARTN_WIN.partnAttrs[i] == op2->u.PARTN_WIN.partnAttrs[i]))
      return false;
  //everything is equal --> the operators are equal
  return true;

}

static bool compareJoins ( Operator *op1, Operator *op2 ){
  //are the operators of type JOIN
  //check the JOIN struct
  if ( op1->kind == PO_JOIN ) {
    //compare numbers
    if ( op1->u.JOIN.numOuterAttrs != op2->u.JOIN.numOuterAttrs)
      return false;
    if ( op1->u.JOIN.numInnerAttrs != op2->u.JOIN.numInnerAttrs )
      return false;
    //compare predicates
    if ( !( *(op1->u.JOIN.pred) == *(op2->u.JOIN.pred )))
      return false;
    return true;
  }
  //they are of type STR_JOIN
  //check the STR_JOIN struct
  else {
    //compare numbers
    if ( op1->u.STR_JOIN.numOuterAttrs != op2->u.STR_JOIN.numOuterAttrs)
      return false;
    if ( op1->u.STR_JOIN.numInnerAttrs != op2->u.STR_JOIN.numInnerAttrs )
      return false;
    //compare predicates
    if ( !( *(op1->u.STR_JOIN.pred) == *(op2->u.STR_JOIN.pred )))
      return false;
    return true;
  }

}

static bool compareJoinProjs ( Operator *op1, Operator *op2 ){
  //are the operators of type JOIN_PROJECT
  if ( op1->kind == PO_JOIN_PROJECT ){
    //check the join predicate
    if ( !(*(op1->u.JOIN_PROJECT.pred) == *(op2->u.JOIN_PROJECT.pred)) )
      return false;

    //compare the project attributes
    int i = 0;
    for ( i=0; i< op1->numAttrs; i++ )
      if ( !(*(op1->u.JOIN_PROJECT.projs[i]) == *(op2->u.JOIN_PROJECT.projs[i])))
	return false;
    return true;
  }
  //they are of type STR_JOIN_PROJECT
  else {
    //check the join predicate
    if ( !(*(op1->u.STR_JOIN_PROJECT.pred) == *(op2->u.STR_JOIN_PROJECT.pred)) )
      return false;

    //compare the project attributes
    int i = 0;
    for ( i=0; i< op1->numAttrs; i++ )
      if ( !(*(op1->u.STR_JOIN_PROJECT.projs[i]) == *(op2->u.STR_JOIN_PROJECT.projs[i])))
	return false;
    return true;    

    
  }

}



static bool compareOperators( Operator *op1, Operator *op2 ){
  if ( op1->id == op2->id )
    return true;
  //if the operator kinds are different then the operators are different
  if ( op1->kind != op2->kind ) 
    return false;
  //printf("comparing operators with equal types");
  //if they have different attributes then return false
  if ( op1->numAttrs != op2->numAttrs )
    return false;
  //printf("comparing operators with equal num attrs \n");
  //do both operators produce streams or not
  if ( op1->bStream != op2->bStream )
    return false;
  // do they have the same number of inputs
  if ( op1->numInputs != op2->numInputs )
    return false;

  //compare the attributes
  int i =0;
  for ( i =0; i < op1->numAttrs; i++) {
    if ( op1->attrTypes[i] != op2->attrTypes[i] )
      return false;
    if ( op1->attrLen[i] != op2->attrLen[i])
      return false;
  }
  //printf("comparing operators with equal attrs %d %d\n", op1->id, op2->id);
  //compare the inputs
  for ( i =0; i < op2->numInputs; i++)
    if ( !compareOperators(op1->inputs[i],op2->inputs[i]) )
      return false;
  //printf("comparing operators with equal inputs %d %d\n", op1->id, op2->id);
  //compare kind specific characteristics
  switch ( op1->kind)
    {
    case PO_SELECT:
      return compareSelects(op1, op2);

    case PO_PROJECT:
      return compareProjects(op1,op2);

    case PO_JOIN: case PO_STR_JOIN:
      return compareJoins(op1, op2);
          
    case PO_JOIN_PROJECT: case PO_STR_JOIN_PROJECT:
      return compareJoinProjs(op1,op2);
      
    case PO_GROUP_AGGR: 
      return compareGroupAggrs(op1,op2);
      
    case PO_DISTINCT:
      return true;
      
    case PO_ROW_WIN: 
      return compareRowWins(op1,op2);
      
    case PO_RANGE_WIN:
      return compareRangeWins(op1,op2);
      
    case PO_PARTN_WIN:
      return comparePartnWins(op1,op2);
      
    case PO_ISTREAM: case PO_DSTREAM: case PO_RSTREAM:
      return true;
      
    case PO_UNION: case PO_EXCEPT:
      return true;
		
    case PO_STREAM_SOURCE: case PO_RELN_SOURCE:
      //we should not reach here unless by error 
      return false;
      
    case PO_OUTPUT:
      //we do not want to combine these right now
      return false;

		
    case PO_SINK:
      //we do not want to combine these right now
      return false;
      
    case PO_SS_GEN:
      //there should be only one in the system 
      //if we reach here --> there is an error
      return false;
    //drop operator, by Thao Pham
    case PO_DROP:
      //we should not come here, since the drop operator is 
      //added after all shared operators finding is finished 
      return false;
      
      //end of drop operator, by Thao Pham
    default:
      //this operator has a type that we don't know
      printf("Unidentified operator");
      return false;
  }

}

static bool mergeOperators ( Operator *op1, Operator *op2, bool sharing_among_class ) {

	//check to see if the two operators belong to 2 different classes?

	/* Thao Pham : now I assume that the class of smaller id had higher priority
	 * this is to be changed to support dynamic changing of user-specified priority at run time
	 */

	fprintf(stdout,"\nid 1: %d; id 2: %d  \n", op1->query_class_id, op2->query_class_id);

	if(op1->query_class_id != op2->query_class_id && !sharing_among_class)
		return 0;

	if(op2->query_class_id < op1->query_class_id)
		op1->query_class_id = op2-> query_class_id;

	int i = op1->numOutputs;
	//add the outputs of op2 to op1
	unsigned int total_outs = op1->numOutputs + op2->numOutputs;
	int j = 0;
  //copy the outputs from op2 to op1
	for ( ; i< total_outs; i++){
		op1->outputs[i] = op2->outputs[j];

		//find op2 in these operators input list
		int found = -1;
		for ( int l=0; l < op2->outputs[j]->numInputs; l++) {
			if ( op2->outputs[j]->inputs[l] == op2 )
			found = l;
		}

		//op2 was not found --> error
		if (found == -1 ){
		  printf("Op2 not found! ERROR!!");
		  return false;
		}

		else {
		  //replace op2 with op1 in the input list of its output operators
		  op2->outputs[j]->inputs[found] = op1;
		}
		j++;

  }
  
  op1->numOutputs = total_outs;

  // remove op2 from the output list of its inputs
  for ( i=0; i < op2->numInputs; i++ ){
    //find op2 in these operators output list
    int found = -1;
    for ( j=0; j< op2->inputs[i]->numOutputs; j++) {
      if ( op2->inputs[i]->outputs[j] == op2 )
	found = j;
    }
    //op2 was not found --> error
    if (found == -1 ){
      printf("Op2 not found! ERROR!!");
      return false;
    }
    else {
      for ( int k = found; k < op2->inputs[i]->numOutputs - 1; k++) {
	op2->inputs[i]->outputs[k] = op2->inputs[i]->outputs[k+1];
      }
      op2->inputs[i]->numOutputs = op2->inputs[i]->numOutputs -1;
    }
    
  }
  
  

  //remove op2 from the linked list
  Operator *next = op2->next;
  op2->prev->next = next;
  next->prev = op2->prev;

  
  //delete op2
  //delete op2;
  
  return true;
  

}

bool findInVector( vector <Operator*> vec, Operator *op ){
  
  int i = 0;
  
  for ( i=0; i < vec.size(); i++ )
    if ( vec[i]->id == op->id ) 
      return true;
  
  return false;

}



bool PlanManagerImpl::findSharedOps () {
  
  Operator *op, *nextOp;
  vector <Operator *> current_ops;
  vector <Operator *> next_ops;
  // Iterate through all the used ops
  op = usedOps;

  printf("Finding shared operators");
  // add the data sources into the current_ops list
  while (op) {
    if ( op->kind == PO_STREAM_SOURCE || op->kind == PO_RELN_SOURCE)
      current_ops.push_back(op);
    
    op = op -> next;
    
  }
  
  while ( current_ops.size() != 0 ) {
    int i =0;
    //iterate over the operators
    for (i =0; i < current_ops.size(); i++){
      for ( int j =i+1; j < current_ops.size(); j++){
	//printf("Comparing operators %d %d \t",
	       //current_ops[i]->id, current_ops[j]->id );
	if (compareOperators( current_ops[i], current_ops[j])){
	  
	  printf("Found two equal operators %d %d \n",
		 current_ops[i]->id, current_ops[j]->id );
	  mergeOperators(current_ops[i], current_ops[j], sharingAmongClasses);
	  current_ops.erase(current_ops.begin()+j);
	  j--;
	}
	
      }//end of j loop
      //put the outputs of this operator into the next_ops list
      for ( int k =0; k < current_ops[i]->numOutputs; k++ )
	next_ops.push_back(current_ops[i]->outputs[k]);
      //remove this operator from the current_ops list
      //current_ops.erase(current_ops.begin() + i);
    } //end of i loop
    current_ops.clear();
    //copy the operators from the next_ops to the current_ops
    for ( i =0; i< next_ops.size(); i++ ) 
      if ( !findInVector( current_ops, next_ops[i]))
	current_ops.push_back(next_ops[i]);
    
    //erase all elements in the next_ops list
    next_ops.clear();



  }//end of while loop

  


  return true;
 
} 
    
//end of modification by LAM

//insert the shedder into the operator(s) belonging to the lower priority class(es) that come after the shared segments between classes
int PlanManagerImpl::insertDropOps()
{
	int rc;
		Operator *op;
		//list of operator that a shedder will be embedded into
		vector <Operator *> candidates;
	  // vector <Operator *> next_ops;
	  // Iterate through all the used ops, put to the list:
	  //-source ops
	  //or op that have more than ops reading from its output AND btream is true
	  //or op that has only one op reading from its output but is the first op above an bstream-is-false op that has more than one op reading from its output


		//find the end of the segments that are shared between classes

		op= usedOps;
		//set the correct class-id for the source

		while(op){

			if(op->kind ==PO_STREAM_SOURCE && op->numOutputs > 1){
				for(int o = 0; o< op->numOutputs; o++)
				if(op->outputs[o]->query_class_id < op->query_class_id){

					op->query_class_id = op->outputs[o]->query_class_id;

				}
			}
			op = op->next;
		}

		op=usedOps;
		while(op){

			if(op->numOutputs > 1 && op -> bStream==true &&op->kind != PO_SS_GEN){ // it would be problematic if an operator is supposed to have a shedder embedded to its outputs but its bstream = false; but I assume this will not happen for now
				for(int o = 0; o< op->numOutputs; o++)
				if(op->outputs[o]->query_class_id > op->query_class_id){
					//the output[] should have a shedded embedded and should be treated as the source operator for the lower-priority class
					op->outputs[o]->instOp->isShedder = true;
					printf("shedder: %d; %d; %d \n", op->id, op->kind, op->query_class_id);

				}
			}
			op = op->next;
		}

		return 0;
}

//------------------------------------------
//insert inactive drop operators, by Thao Pham
////This version embed the shedding into the source operator
/* int PlanManagerImpl::insertDropOps()
{
	int rc;
	Operator *op;
	
	// In this version, we just put the drop operators at every input (after the stream source operator)
	// The code to insert the drop to other places in the query network will be moved to the next version ofAQSIOS

	 op = usedOps;
  	 unsigned int inputIdx;
  	 while(op){
  			if(op->kind == PO_STREAM_SOURCE )
  				for (int k=0; k< op -> numOutputs; k++){
					//it makes no sense to put a drop if we have already reached the output
					if(op -> outputs [k]->kind!=PO_SINK && op -> outputs [k]-> kind != PO_OUTPUT)  
					{	Operator *drop;
			  			if((rc= mk_drop(op,drop)!=0))
			  			return rc;
			  			
			  			ASSERT (op -> outputs [op -> numOutputs - 1] == drop);
						ASSERT (drop -> inputs [0] == op);
						
						inputIdx = getInputIndex(op->outputs[k],op);
						
						ASSERT (op -> outputs [k] -> numInputs > inputIdx);
						ASSERT (op -> outputs [k] -> inputs [inputIdx] == op);
						
						// change the order in the usedOps list so that the drop
						 // comes after its output op (yes, after, since it is a reverse order)
						//
						if(drop->prev)
							drop->prev->next = drop->next;
						drop->next->prev = drop->prev;
						
						if(usedOps==drop) usedOps = drop->next;
						
						if(op->outputs[k]->next)
							op->outputs[k]->next->prev = drop;
						
						drop->next = op->outputs[k]->next;
						op->outputs[k]->next = drop;
						drop->prev = op->outputs[k];
						
						//update the semantic link in the query tree
						op -> outputs [k] -> inputs[inputIdx] = drop;
						
						rc = addOutput (drop, op -> outputs [k]);
						
						if (rc != 0)
							return rc;	
						op -> outputs [k] = drop;
						op -> numOutputs--;
							
					}
				}	
  		
  		op = op->next;			
  		
  	}
  	
	return 0;
}
//insert inactive drop operators, by Thao Pham
*/

