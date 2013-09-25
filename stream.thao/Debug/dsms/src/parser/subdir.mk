################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../dsms/src/parser/nodes.o \
../dsms/src/parser/parse.o \
../dsms/src/parser/scan.o \
../dsms/src/parser/scanhelp.o 

CC_SRCS += \
../dsms/src/parser/nodes.cc \
../dsms/src/parser/parse.cc \
../dsms/src/parser/scan.cc \
../dsms/src/parser/scanhelp.cc 

OBJS += \
./dsms/src/parser/nodes.o \
./dsms/src/parser/parse.o \
./dsms/src/parser/scan.o \
./dsms/src/parser/scanhelp.o 

CC_DEPS += \
./dsms/src/parser/nodes.d \
./dsms/src/parser/parse.d \
./dsms/src/parser/scan.d \
./dsms/src/parser/scanhelp.d 


# Each subdirectory must supply rules for building sources it contributes
dsms/src/parser/%.o: ../dsms/src/parser/%.cc
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -D_DM_ -D_LOAD_MANAGE_ -D_CTRL_LOAD_MANAGE_ -D_SYS_STR_ -D_MONITOR_ -I"/home/thao/workspace/p3-aqsios/stream.thao/dsms/include" -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o"$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


