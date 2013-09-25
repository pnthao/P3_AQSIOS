################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../dsms/src/execution/synopses/lin_syn_impl.o \
../dsms/src/execution/synopses/partn_win_syn_impl.o \
../dsms/src/execution/synopses/rel_syn_impl.o \
../dsms/src/execution/synopses/win_syn_impl.o 

CC_SRCS += \
../dsms/src/execution/synopses/lin_syn_impl.cc \
../dsms/src/execution/synopses/partn_win_syn_impl.cc \
../dsms/src/execution/synopses/rel_syn_impl.cc \
../dsms/src/execution/synopses/win_syn_impl.cc 

OBJS += \
./dsms/src/execution/synopses/lin_syn_impl.o \
./dsms/src/execution/synopses/partn_win_syn_impl.o \
./dsms/src/execution/synopses/rel_syn_impl.o \
./dsms/src/execution/synopses/win_syn_impl.o 

CC_DEPS += \
./dsms/src/execution/synopses/lin_syn_impl.d \
./dsms/src/execution/synopses/partn_win_syn_impl.d \
./dsms/src/execution/synopses/rel_syn_impl.d \
./dsms/src/execution/synopses/win_syn_impl.d 


# Each subdirectory must supply rules for building sources it contributes
dsms/src/execution/synopses/%.o: ../dsms/src/execution/synopses/%.cc
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -D_DM_ -D_CTRL_LOAD_MANAGE_ -D_LOAD_MANAGE_ -D_SYS_STR_ -D_MONITOR_ -I"/home/thao/workspace/stream.thao/dsms/include" -O3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o"$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


