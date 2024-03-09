# Databricks-Notebook-DAG-Execution

# Problem Statement

![Alt text](/images/job_flows.png)

Let's understand the problem statement.
Look at the above photo for a Sample ETL, which represents any DAG-based ETL like AbInitio, Informatica, Pentaho, and Datastage jobs, etc., where a job contains a set of steps to be executed, and steps have defined dependencies.
There are a few important corner cases to cover:

- Branch at Step 2: Step 3 and Step 4 once Step 2 is complete.
- Conditional Flow at Step 3: Step 5 triggers if Step 3 is successful, and Step 7 triggers when Step 3 fails.
- Merge at Step 10: Step 10 will be executed once Steps 6, 8, & 9 are completed.
- 
Now, we are asked to migrate into PySpark notebook. This is also applicable when you are creating fresh notebooks with such steps with dependencies.

# Solution

There are two options available.
## Option 1 : Topological Sorted ordered Steps in Notebook

  Easiest way to implement:
  - Perform a topological sort (Step 1 > Step 2 > Step 3 > Step 5 > Step 6 > Step 7 > Step 8 > Step 4 > Step 9 > Step 10 > Step 11) on all steps.
  - Place steps' source code in ordered notebook commands.
    
  There are multiple shortcomings in this approach:
  - No Parallelism: All steps are executed in sequence even if there's no dependency.
  - Doesn't cover conditional execution (Example: Step 5 triggers if Step 3 is successful, and Step 7 triggers when Step 3 fails).
  - Error handling: If any step fails, it stops execution for all steps, even for steps on which it does not depend.
    
These limitations we will try to solve with option 2.
     
## Option 2 : Trigger based DAG execution for Job Steps

     
  The idea is simple:
  
  - Transform individual steps in legacy code into PySpark code and create separate methods for each step, like Step1(), Step2(), etc.
  - Then, the Job Executor triggers only the starting point, i.e., Step1.
  - Each step, when it completes execution, triggers its successors:
    - If all successors need to be triggered, trigger them all.
    - If successors need to be triggered based on condition, trigger them based on the condition.
  - If triggered by any parent step, execute logic only when all parent steps are completed. See Step 10, which depends on Steps 6, 8, & 9.
  
  I have created multiple notebook-based implementations below.
           
# Implementation
I have implemented Databricks Notebook however this can be followed to create other Notebooks like Glue, Jupyter etc.

## Python Notebook Using Class 

  [Python-Notebook-Using-Class](notebooks/Python-Notebook-Using-Class.ipynb)

    - Uses Python Class for Job packaging
    - DAG Based execution.
    - Error Handling
    - Parallel execution
    
## Python Notebook Using Commands

[Python-Notebook-Using-Class](notebooks/Python-Notebook-Using-Commands.ipynb)

    - Leverages Notebook Commands for defining Job's steps.
    - DAG Based execution.
    - Error Handling
    - Parallel execution

## Simple Python Notebook ( just my initial Version) 

[Python-Notebook-Using-Class](notebooks/Python-Notebook-V1.ipynb)


# TODO:
 * Parallel execution.
   
