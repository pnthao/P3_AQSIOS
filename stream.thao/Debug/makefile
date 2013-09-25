################################################################################
# Automatically-generated file. Do not edit!
################################################################################

-include ../makefile.init

RM := rm -rf

# All of the sources participating in the build are defined here
-include sources.mk
-include subdir.mk
-include gen_client/subdir.mk
-include dsms/src/server/subdir.mk
-include dsms/src/server/.libs/subdir.mk
-include dsms/src/querygen/subdir.mk
-include dsms/src/querygen/.libs/subdir.mk
-include dsms/src/parser/subdir.mk
-include dsms/src/parser/.libs/subdir.mk
-include dsms/src/metadata/subdir.mk
-include dsms/src/metadata/.libs/subdir.mk
-include dsms/src/interface/subdir.mk
-include dsms/src/interface/.libs/subdir.mk
-include dsms/src/execution/synopses/subdir.mk
-include dsms/src/execution/synopses/.libs/subdir.mk
-include dsms/src/execution/stores/subdir.mk
-include dsms/src/execution/stores/.libs/subdir.mk
-include dsms/src/execution/scheduler/subdir.mk
-include dsms/src/execution/scheduler/.libs/subdir.mk
-include dsms/src/execution/queues/subdir.mk
-include dsms/src/execution/queues/.libs/subdir.mk
-include dsms/src/execution/operators/subdir.mk
-include dsms/src/execution/operators/.libs/subdir.mk
-include dsms/src/execution/monitors/subdir.mk
-include dsms/src/execution/monitors/.libs/subdir.mk
-include dsms/src/execution/memory/subdir.mk
-include dsms/src/execution/memory/.libs/subdir.mk
-include dsms/src/execution/loadmanager/subdir.mk
-include dsms/src/execution/loadmanager/.libs/subdir.mk
-include dsms/src/execution/internals/subdir.mk
-include dsms/src/execution/internals/.libs/subdir.mk
-include dsms/src/execution/indexes/subdir.mk
-include dsms/src/execution/indexes/.libs/subdir.mk
-include dsms/src/execution/communicator/subdir.mk
-include dsms/src/execution/communicator/.libs/subdir.mk
-include dsms/src/common/subdir.mk
-include dsms/src/common/.libs/subdir.mk
-include objects.mk

ifneq ($(MAKECMDGOALS),clean)
ifneq ($(strip $(C++_DEPS)),)
-include $(C++_DEPS)
endif
ifneq ($(strip $(C_DEPS)),)
-include $(C_DEPS)
endif
ifneq ($(strip $(CC_DEPS)),)
-include $(CC_DEPS)
endif
ifneq ($(strip $(CPP_DEPS)),)
-include $(CPP_DEPS)
endif
ifneq ($(strip $(CXX_DEPS)),)
-include $(CXX_DEPS)
endif
ifneq ($(strip $(C_UPPER_DEPS)),)
-include $(C_UPPER_DEPS)
endif
endif

-include ../makefile.defs

# Add inputs and outputs from these tool invocations to the build variables 

# All Target
all: stream.thao.purdue

# Tool invocations
stream.thao.purdue: $(OBJS) $(USER_OBJS)
	@echo 'Building target: $@'
	@echo 'Invoking: GCC C++ Linker'
	g++  -o"stream.thao.purdue" $(OBJS) $(USER_OBJS) $(LIBS)
	@echo 'Finished building target: $@'
	@echo ' '

# Other Targets
clean:
	-$(RM) $(OBJS)$(C++_DEPS)$(C_DEPS)$(CC_DEPS)$(CPP_DEPS)$(EXECUTABLES)$(CXX_DEPS)$(C_UPPER_DEPS) stream.thao.purdue
	-@echo ' '

.PHONY: all clean dependents
.SECONDARY:

-include ../makefile.targets
