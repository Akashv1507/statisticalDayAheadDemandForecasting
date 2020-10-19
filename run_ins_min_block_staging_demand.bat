call project_env\Scripts\activate.bat
call python index_insMinwiseDemand.py
call python index_insBlockwiseDemand.py
call python index_insStagingBlockwiseDemand.py --want_updation True
pause