################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../dsms/src/metadata/gen_phy_plan.o \
../dsms/src/metadata/gen_xml_plan.o \
../dsms/src/metadata/inst_aggr.o \
../dsms/src/metadata/inst_dist.o \
../dsms/src/metadata/inst_drop.o \
../dsms/src/metadata/inst_except.o \
../dsms/src/metadata/inst_expr.o \
../dsms/src/metadata/inst_join.o \
../dsms/src/metadata/inst_lin_store.o \
../dsms/src/metadata/inst_output.o \
../dsms/src/metadata/inst_project.o \
../dsms/src/metadata/inst_pwin.o \
../dsms/src/metadata/inst_pwin_store.o \
../dsms/src/metadata/inst_range_win.o \
../dsms/src/metadata/inst_rel_source.o \
../dsms/src/metadata/inst_rel_store.o \
../dsms/src/metadata/inst_row_win.o \
../dsms/src/metadata/inst_rstream.o \
../dsms/src/metadata/inst_select.o \
../dsms/src/metadata/inst_simple_store.o \
../dsms/src/metadata/inst_sink.o \
../dsms/src/metadata/inst_ss_gen.o \
../dsms/src/metadata/inst_str_join.o \
../dsms/src/metadata/inst_str_source.o \
../dsms/src/metadata/inst_union.o \
../dsms/src/metadata/inst_win_store.o \
../dsms/src/metadata/inst_xstream.o \
../dsms/src/metadata/phy_op_debug.o \
../dsms/src/metadata/plan_inst.o \
../dsms/src/metadata/plan_mgr.o \
../dsms/src/metadata/plan_mgr_impl.o \
../dsms/src/metadata/plan_mgr_monitor.o \
../dsms/src/metadata/plan_queue.o \
../dsms/src/metadata/plan_store.o \
../dsms/src/metadata/plan_syn.o \
../dsms/src/metadata/plan_trans.o \
../dsms/src/metadata/query_mgr.o \
../dsms/src/metadata/static_tuple_alloc.o \
../dsms/src/metadata/table_mgr.o \
../dsms/src/metadata/tuple_layout.o 

CC_SRCS += \
../dsms/src/metadata/gen_phy_plan.cc \
../dsms/src/metadata/gen_xml_plan.cc \
../dsms/src/metadata/inst_aggr.cc \
../dsms/src/metadata/inst_dist.cc \
../dsms/src/metadata/inst_drop.cc \
../dsms/src/metadata/inst_except.cc \
../dsms/src/metadata/inst_expr.cc \
../dsms/src/metadata/inst_join.cc \
../dsms/src/metadata/inst_lin_store.cc \
../dsms/src/metadata/inst_output.cc \
../dsms/src/metadata/inst_project.cc \
../dsms/src/metadata/inst_pwin.cc \
../dsms/src/metadata/inst_pwin_store.cc \
../dsms/src/metadata/inst_range_win.cc \
../dsms/src/metadata/inst_rel_source.cc \
../dsms/src/metadata/inst_rel_store.cc \
../dsms/src/metadata/inst_row_win.cc \
../dsms/src/metadata/inst_rstream.cc \
../dsms/src/metadata/inst_select.cc \
../dsms/src/metadata/inst_simple_store.cc \
../dsms/src/metadata/inst_sink.cc \
../dsms/src/metadata/inst_ss_gen.cc \
../dsms/src/metadata/inst_str_join.cc \
../dsms/src/metadata/inst_str_source.cc \
../dsms/src/metadata/inst_union.cc \
../dsms/src/metadata/inst_win_store.cc \
../dsms/src/metadata/inst_xstream.cc \
../dsms/src/metadata/phy_op_debug.cc \
../dsms/src/metadata/plan_inst.cc \
../dsms/src/metadata/plan_mgr.cc \
../dsms/src/metadata/plan_mgr_impl.cc \
../dsms/src/metadata/plan_mgr_monitor.cc \
../dsms/src/metadata/plan_queue.cc \
../dsms/src/metadata/plan_store.cc \
../dsms/src/metadata/plan_syn.cc \
../dsms/src/metadata/plan_trans.cc \
../dsms/src/metadata/query_mgr.cc \
../dsms/src/metadata/static_tuple_alloc.cc \
../dsms/src/metadata/table_mgr.cc \
../dsms/src/metadata/tuple_layout.cc 

OBJS += \
./dsms/src/metadata/gen_phy_plan.o \
./dsms/src/metadata/gen_xml_plan.o \
./dsms/src/metadata/inst_aggr.o \
./dsms/src/metadata/inst_dist.o \
./dsms/src/metadata/inst_drop.o \
./dsms/src/metadata/inst_except.o \
./dsms/src/metadata/inst_expr.o \
./dsms/src/metadata/inst_join.o \
./dsms/src/metadata/inst_lin_store.o \
./dsms/src/metadata/inst_output.o \
./dsms/src/metadata/inst_project.o \
./dsms/src/metadata/inst_pwin.o \
./dsms/src/metadata/inst_pwin_store.o \
./dsms/src/metadata/inst_range_win.o \
./dsms/src/metadata/inst_rel_source.o \
./dsms/src/metadata/inst_rel_store.o \
./dsms/src/metadata/inst_row_win.o \
./dsms/src/metadata/inst_rstream.o \
./dsms/src/metadata/inst_select.o \
./dsms/src/metadata/inst_simple_store.o \
./dsms/src/metadata/inst_sink.o \
./dsms/src/metadata/inst_ss_gen.o \
./dsms/src/metadata/inst_str_join.o \
./dsms/src/metadata/inst_str_source.o \
./dsms/src/metadata/inst_union.o \
./dsms/src/metadata/inst_win_store.o \
./dsms/src/metadata/inst_xstream.o \
./dsms/src/metadata/phy_op_debug.o \
./dsms/src/metadata/plan_inst.o \
./dsms/src/metadata/plan_mgr.o \
./dsms/src/metadata/plan_mgr_impl.o \
./dsms/src/metadata/plan_mgr_monitor.o \
./dsms/src/metadata/plan_queue.o \
./dsms/src/metadata/plan_store.o \
./dsms/src/metadata/plan_syn.o \
./dsms/src/metadata/plan_trans.o \
./dsms/src/metadata/query_mgr.o \
./dsms/src/metadata/static_tuple_alloc.o \
./dsms/src/metadata/table_mgr.o \
./dsms/src/metadata/tuple_layout.o 

CC_DEPS += \
./dsms/src/metadata/gen_phy_plan.d \
./dsms/src/metadata/gen_xml_plan.d \
./dsms/src/metadata/inst_aggr.d \
./dsms/src/metadata/inst_dist.d \
./dsms/src/metadata/inst_drop.d \
./dsms/src/metadata/inst_except.d \
./dsms/src/metadata/inst_expr.d \
./dsms/src/metadata/inst_join.d \
./dsms/src/metadata/inst_lin_store.d \
./dsms/src/metadata/inst_output.d \
./dsms/src/metadata/inst_project.d \
./dsms/src/metadata/inst_pwin.d \
./dsms/src/metadata/inst_pwin_store.d \
./dsms/src/metadata/inst_range_win.d \
./dsms/src/metadata/inst_rel_source.d \
./dsms/src/metadata/inst_rel_store.d \
./dsms/src/metadata/inst_row_win.d \
./dsms/src/metadata/inst_rstream.d \
./dsms/src/metadata/inst_select.d \
./dsms/src/metadata/inst_simple_store.d \
./dsms/src/metadata/inst_sink.d \
./dsms/src/metadata/inst_ss_gen.d \
./dsms/src/metadata/inst_str_join.d \
./dsms/src/metadata/inst_str_source.d \
./dsms/src/metadata/inst_union.d \
./dsms/src/metadata/inst_win_store.d \
./dsms/src/metadata/inst_xstream.d \
./dsms/src/metadata/phy_op_debug.d \
./dsms/src/metadata/plan_inst.d \
./dsms/src/metadata/plan_mgr.d \
./dsms/src/metadata/plan_mgr_impl.d \
./dsms/src/metadata/plan_mgr_monitor.d \
./dsms/src/metadata/plan_queue.d \
./dsms/src/metadata/plan_store.d \
./dsms/src/metadata/plan_syn.d \
./dsms/src/metadata/plan_trans.d \
./dsms/src/metadata/query_mgr.d \
./dsms/src/metadata/static_tuple_alloc.d \
./dsms/src/metadata/table_mgr.d \
./dsms/src/metadata/tuple_layout.d 


# Each subdirectory must supply rules for building sources it contributes
dsms/src/metadata/%.o: ../dsms/src/metadata/%.cc
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -D_DM_ -D_LOAD_MANAGE_ -D_CTRL_LOAD_MANAGE_ -D_SYS_STR_ -D_MONITOR_ -I"/home/thao/PITT/workspace/p3/P3_AQSIOS/stream.thao/dsms/include" -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


