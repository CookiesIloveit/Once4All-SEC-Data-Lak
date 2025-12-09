import os
import gc
import re
import time
import orjson
import psutil
import pyodbc
import datetime
from dotenv import load_dotenv
import matplotlib.pyplot as plt
from multiprocessing import cpu_count
from concurrent.futures import ProcessPoolExecutor, as_completed

# --------------------
# Configuration
# --------------------
load_dotenv()

SubMDIRPATH = os.getenv('PATH_SUBMISSIONS', r"C:\Default\Path")
FactDIRPATH = os.getenv('PATH_FACTS', r"C:\Default\Path")

dbServer = os.getenv('DB_SERVER', 'localhost')
dbName = os.getenv('DB_NAME', 'Hippocampus')

CONN_STR = (f"DRIVER={{ODBC Driver 18 for SQL Server}};"
           f"SERVER={dbServer};"
           f"DATABASE={dbName};"
           "Trusted_Connection=yes;"
           "TrustServerCertificate=yes;"
           "Encrypt=no;"  
           "Connection Timeout=600;"  
           "Query Timeout=600;")


# ---------------------------------------------------------
# Shared: Performance Reporting Function
# ---------------------------------------------------------
def generatePerformanceReport(timestamps, throughputs, cpu_usages, ram_usages, remaining_tasks,
                              db_wait_times, ctx_switches, total_tasks, report_name="Performance", unit_label="Files"):
    try:
        if not timestamps:
            print(f"\n[Graph Error] No data points collected for {report_name}.")
            return

        plt.style.use('ggplot')
        fig, axs = plt.subplots(3, 2, figsize=(15, 15))

        # [Dynamic Title] ใช้ unit_label แสดงหน่วยนับ
        fig.suptitle(f'{report_name} Report (Total: {total_tasks} {unit_label})', fontsize=16)

        # 1. Throughput
        axs[0, 0].plot(timestamps, throughputs, color='tab:blue')
        axs[0, 0].set_title(f'Throughput ({unit_label} / sec)')  # <--- Dynamic Label
        axs[0, 0].set_ylabel(f'{unit_label}/sec')
        axs[0, 0].grid(True)

        # 2. System Resources
        axs[0, 1].plot(timestamps, cpu_usages, color='tab:red', label='CPU %')
        axs[0, 1].plot(timestamps, ram_usages, color='tab:orange', label='RAM %')
        axs[0, 1].set_title('System Resource Usage')
        axs[0, 1].set_ylabel('Percent (%)')
        axs[0, 1].legend()
        axs[0, 1].grid(True)

        # 3. Work Burn-down
        axs[1, 0].plot(timestamps, remaining_tasks, color='tab:green')
        axs[1, 0].fill_between(timestamps, remaining_tasks, color='tab:green', alpha=0.3)
        axs[1, 0].set_title(f'Work Burn-down (Remaining {unit_label})')  # <--- Dynamic Label
        axs[1, 0].set_ylabel(f'{unit_label} Count')
        axs[1, 0].grid(True)

        # 4. DB Wait Time
        axs[1, 1].plot(timestamps, db_wait_times, color='tab:purple')
        axs[1, 1].set_title('Database Write Time per Batch')
        axs[1, 1].set_ylabel('Seconds (Wait)')
        axs[1, 1].set_xlabel('Time (Seconds)')
        axs[1, 1].grid(True)

        # 5. Context Switches
        axs[2, 0].plot(timestamps, ctx_switches, color='tab:brown')
        axs[2, 0].set_title('Context Switches per Second')
        axs[2, 0].set_ylabel('Switches / sec')
        axs[2, 0].set_xlabel('Time (Seconds)')
        axs[2, 0].grid(True)

        # 6. CPU Idle
        cpu_idle = [100 - x for x in cpu_usages]
        axs[2, 1].plot(timestamps, cpu_idle, color='gray')
        axs[2, 1].set_title('CPU Idle % (Potential I/O Wait)')
        axs[2, 1].set_ylabel('Idle %')
        axs[2, 1].set_xlabel('Time (Seconds)')
        axs[2, 1].grid(True)

        filename = f"{report_name}_report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        plt.tight_layout()
        plt.savefig(filename)
        print(f"\nPerformance Graph saved as: {filename}")

    except Exception as e:
        print(f"\nError generating graph: {e}")
# ---------------------------------------------------------
# PART 1: SECDataLake (Facts) Processing
# ---------------------------------------------------------
def prepareFactData(fileName):
    # pid = os.getpid()
    cik = fileName.replace("CIK", "").replace(".json", "")

    try:
        filePath = os.path.join(FactDIRPATH, fileName)

        # 1. อ่านไฟล์ (Binary Mode)
        with open(filePath, "rb") as f:
            rawData = f.read()

        # 2. Validate JSON
        try:
            jsonData = orjson.loads(rawData)

            if (not jsonData or
                    'entityName' not in jsonData or
                    'facts' not in jsonData or
                    not jsonData['facts']):
                return (cik, None, f"Invalid JSON Structure")

        except orjson.JSONDecodeError:
            return (cik, None, f"Corrupted JSON")

        # 3. แปลงเป็น String พร้อมส่ง
        jsonStr = rawData.decode('utf-8')

        # ส่งข้อมูลกลับ (Tuple: CIK, JSON, Error)
        return (cik, jsonStr, None)

    except Exception as e:
        return (cik, None, str(e))


# ---------------------------------------------------------
# Main Process: จัดการ Batch Insert ลง SECDataLake
# ---------------------------------------------------------
def fetchFactBulkData():
    # ใช้ Worker ได้เต็มที่ 32+4 Core
    maxWorkers = min(32, cpu_count() + 4)
    print(f"[SECDataLake] Spawning {maxWorkers} workers (Producer-Consumer)...")
    print("Status: Scanning dir...")

    allFiles = [f for f in os.listdir(FactDIRPATH) if f.endswith(".json")]

    # Check Existing Data
    print(f"Status: Checking existing data in DB...")
    tempConn = pyodbc.connect(CONN_STR)
    tempCursor = tempConn.cursor()
    tempCursor.execute("SELECT cik FROM SECDataLake")
    completeCiks = {row.cik for row in tempCursor.fetchall()}
    tempConn.close()

    # Prepare Tasks
    tasks = []
    for fileName in allFiles:
        cik = fileName.replace("CIK", "").replace(".json", "")
        if cik in completeCiks:
            continue
        tasks.append(fileName)

    totalTasks = len(tasks)
    if totalTasks == 0:
        print(f"All Fact files are already in Database.")
        return

    print(f"Total Tasks: {totalTasks} Fact files to process.")
    print("Status: Starting Parallel Processing...")

    # ---------------------------------------------------------
    # เริ่ม Consumer (Database Writer)
    # ---------------------------------------------------------
    conn = pyodbc.connect(CONN_STR)
    conn.autocommit = False
    cur = conn.cursor()
    cur.fast_executemany = True

    batchCountLimit = 1000
    batchSizeLimitMB = 1500
    currentBatchSizeByte = 0
    TASKCHUNKSIZE = 20000
    gcCounter = 0
    GCINTERVAL = 5
    dbBuffer = []

    # ตัวแปรสถิติ
    completedCount = 0
    skippedCount = 0
    statsTimeStamps, statsThroughput, statsCpu, statsRam, statsRemaining, statsDbWait, statsCtxSwitches = [], [], [], [], [], [], []

    # SQL สำหรับ Batch Merge
    sqlBulkMerge = """
        MERGE INTO SECDataLake AS target
        USING (VALUES (?, ?, CURRENT_TIMESTAMP)) AS source(cik, jsonBody, lastUpdated)
        ON target.cik = source.cik
        WHEN MATCHED THEN
            UPDATE SET jsonBody = source.jsonBody, lastUpdated = source.lastUpdated
        WHEN NOT MATCHED THEN
            INSERT (cik, jsonBody, lastUpdated) VALUES (source.cik, source.jsonBody, source.lastUpdated);
    """

    startTime = time.time()
    lastCTXSwitchCount = psutil.cpu_stats().ctx_switches
    lastCheckTime = startTime

    for i in range(0, len(tasks), TASKCHUNKSIZE):
        currentTaskChunk = tasks[i: i + TASKCHUNKSIZE]

        with ProcessPoolExecutor(max_workers=maxWorkers) as executor:
            # ส่งงาน (fileName) ไปให้ Worker
            futures = {executor.submit(prepareFactData, f): f for f in currentTaskChunk}

            for future in as_completed(futures):
                cik, jsonData, error = future.result()

                # 1. เช็ค Error
                if error:
                    skippedCount += 1
                    # print(f"\n[Skip] CIK {cik}: {error}")
                    continue

                itemSize = len(jsonData)
                dbBuffer.append((cik, jsonData))
                currentBatchSizeByte += itemSize

                isCountLimit = len(dbBuffer) >= batchCountLimit
                isSizeLimit = (currentBatchSizeByte / (1024 ** 2)) >= batchSizeLimitMB
                completedCount += 1

                currentTime = time.time()
                elapsed = currentTime - startTime
                speed = completedCount / elapsed if elapsed > 0 else 0
                totalProcessed = completedCount + skippedCount
                if completedCount % 100 == 0 or isCountLimit or isSizeLimit:
                    print(
                        f"    Progress: {completedCount}/{totalTasks} ({speed:.2f} Files/sec) | DB Buffer: {len(dbBuffer)}   ",
                        end="\r")

                if isCountLimit or isSizeLimit:
                    try:
                        timeDBstart = time.time()

                        cur.executemany(sqlBulkMerge, dbBuffer)
                        conn.commit()

                        timeDBend = time.time()
                        dbWriteDuration = timeDBend - timeDBstart


                        dbBuffer.clear()
                        currentBatchSizeByte = 0

                        gcCounter += 1
                        if gcCounter >= GCINTERVAL:
                            gc.collect()
                            gcCounter = 0

                        currentTime = time.time()
                        elapsed = currentTime - startTime
                        speed = completedCount / elapsed if elapsed > 0 else 0
                        remaining = totalTasks - completedCount

                        # Calculate Context Switches Rate
                        currentCTXSwitch = psutil.cpu_stats().ctx_switches
                        timeDiff = currentTime - lastCheckTime
                        ctxRate = 0

                        gcCounter += 1
                        if timeDiff > 0:
                            ctxRate = (currentCTXSwitch - lastCTXSwitchCount) / timeDiff

                        lastCTXSwitchCount = currentCTXSwitch
                        lastCheckTime = currentTime

                        statsTimeStamps.append(elapsed)
                        statsThroughput.append(speed)
                        statsRemaining.append(remaining)
                        statsCpu.append(psutil.cpu_percent())
                        statsRam.append(psutil.virtual_memory().percent)
                        statsDbWait.append(dbWriteDuration)
                        statsCtxSwitches.append(ctxRate)

                        totalProcessed = completedCount + skippedCount
                        print(
                            f"    Progress: {totalProcessed}/{totalTasks} Success: {completedCount} | Skipped: {skippedCount} | Speed: {speed:.2f}/s",
                            end="\r")

                    except Exception as e:
                        print(f"\n[DB Error] Batch Failed: {e}")
                        conn.rollback()

    if dbBuffer:
        try:
            print(f"\nStatus: Writing final batch of {len(dbBuffer)} records...")
            cur.executemany(sqlBulkMerge, dbBuffer)
            conn.commit()

        except Exception as e:
            print(f"\n[DB Error] Final Batch: {e}")

    conn.close()

    totalTime = time.time() - startTime
    print(f"\n\n[Final] Job Done! Processed {completedCount} CIKs in {totalTime:.2f} seconds.")

    try:
        return (statsTimeStamps, statsThroughput, statsCpu, statsRam, statsRemaining, statsDbWait,
                statsCtxSwitches, totalTasks)
    except Exception as e:
        print(f"Graph Error: {e}")


def prepareSubmissionData(cik, fileInfo):
    filesProcessed = 1
    try:
        mainFile = fileInfo["main"]
        chunkFiles = fileInfo["chunks"]
        basePath = os.path.join(SubMDIRPATH, mainFile)

        with open(basePath, "rb") as f:
            mainData = orjson.loads(f.read())

        if "filings" not in mainData or "recent" not in mainData["filings"]:
            return (cik, None, "Invalid structure")

        targetRecent = mainData["filings"]["recent"]
        chunkFiles.sort()

        for chunk in chunkFiles:
            filesProcessed += 1
            chunkPath = os.path.join(SubMDIRPATH, chunk)
            with open(chunkPath, "rb") as f:
                chunkData = orjson.loads(f.read())
            for key, valueList in chunkData.items():
                if key in targetRecent and isinstance(valueList, list):
                    targetRecent[key].extend(valueList)

        jsonString = orjson.dumps(mainData).decode('utf-8')
        return (cik, jsonString, None, filesProcessed)

    except Exception as e:
        return (cik, None, str(e), filesProcessed)


def fetchSubmissionsBulkData():
    # ----init grouping files----#
    print("Status: Scanning files and Grouping...")
    allFiles = [f for f in os.listdir(SubMDIRPATH) if f.endswith(".json")]
    cikGroups = {}
    cikPattern = re.compile(r"CIK(\d{10})")

    for file in allFiles:
        match = cikPattern.match(file)
        if match:
            cik = match.group(1)
            if cik not in cikGroups:
                cikGroups[cik] = {'main': None, 'chunks': []}
            if "submissions-" in file:
                cikGroups[cik]["chunks"].append(file)
            else:
                cikGroups[cik]["main"] = file
    if cikGroups:
        del allFiles
        del cikPattern

    conn = pyodbc.connect(CONN_STR)
    cur = conn.cursor()
    conn.timeout = 600
    conn.autocommit = False
    cur.fast_executemany = True
    print(f"Status: Checking existing data in DB...")
    try:
        cur.execute("SELECT cik FROM SubmissionsDataLake")
        completeCik = {row.cik for row in cur.fetchall()}
    except Exception as e:
        print(f"Status:[DB Error] Could not fetch existing data from Submissions Data Lake: {e}")
        completeCik = set()

    tasks = []
    totalFilesCount = 0

    for cik, info in cikGroups.items():
        if cik in completeCik: continue
        if not info['main']: continue
        fileCountInTask = 1 + len(info['chunks'])
        tasks.append((cik, info, fileCountInTask))
        totalFilesCount += fileCountInTask
    totalTasks = len(tasks)

    if totalFilesCount == 0:
        print(f"All files are already in Database.")
        return

    print(f"Total Workload: {len(tasks)} CIKs composed of {totalFilesCount} Files.")

    # ---tuning Params---#
    batchCountLimit = 20000
    batchSizeLimitMB = 1500
    currentBatchSizeByte = 0
    SUBMITCHUNK = 15000

    gcCounter = 0
    GCINTERVAL = 10

    completedFilesCount = 0
    skippedCount = 0

    dbBuffer = []
    statsTimeStamps, statsThroughput, statsCpu, statsRam, statsRemaining, statsDbWait, statsCtxSwitches = [], [], [], [], [], [], []

    sqlBulkMerge = """
            MERGE INTO SubmissionsDataLake WITH (TABLOCK) AS target
            USING (VALUES (?, ?, CURRENT_TIMESTAMP)) AS source(cik, jsonBody, lastUpdated)
            ON target.cik = source.cik
            WHEN MATCHED THEN
                UPDATE SET jsonBody = source.jsonBody, lastUpdated = source.lastUpdated
            WHEN NOT MATCHED THEN
                INSERT (cik, jsonBody, lastUpdated) VALUES (source.cik, source.jsonBody, source.lastUpdated);
        """
    startTime = time.time()
    lastCTXSwitchCount = psutil.cpu_stats().ctx_switches
    lastCheckTime = startTime

    maxWorkers = min(32, cpu_count() + 4)
    print(f"Spawning {maxWorkers} workers...")
    print("Status: Starting Parallel Processing...")
    for i in range(0, len(tasks), SUBMITCHUNK):
        currentTaskChunk = tasks[i: i + SUBMITCHUNK]  # input: Tasks i to SUBMITCHUNK

        # print(f"\nProcessing Chunk {i}...")

        with ProcessPoolExecutor(max_workers=maxWorkers) as executor:
            futures = {executor.submit(prepareSubmissionData, t[0], t[1]): t[2] for t in currentTaskChunk}

            for future in as_completed(futures):
                cik, jsonData, error, filesDone = future.result()
                completedFilesCount += filesDone

                if error:
                    skippedCount += 1
                    # print(f"\n[Skip] CIK {cik}: {error}")
                    continue

                itemSize = len(jsonData)
                dbBuffer.append((cik, jsonData))
                currentBatchSizeByte += itemSize

                isCountLimit = len(dbBuffer) >= batchCountLimit
                isSizeLimit = (currentBatchSizeByte / (1024 * 1024)) >= batchSizeLimitMB

                currentTime = time.time()
                elapsed = currentTime - startTime
                speed = completedFilesCount / elapsed if elapsed > 0 else 0
                totalProcessed = completedFilesCount + skippedCount

                if completedFilesCount % 100 == 0 or isCountLimit or isSizeLimit:
                    buffer_status = f"| Buffer: {len(dbBuffer)}"
                    print(
                        f"    Progress: {totalProcessed}/{totalFilesCount} Success: {completedFilesCount} | Skipped: {skippedCount} | Speed: {speed:.2f}/s {buffer_status}   ",
                        end="\r")

                if isCountLimit or isSizeLimit:
                    try:
                        # ----------------------------------------------
                        # Measure DB Wait Time
                        # ----------------------------------------------
                        timeDBstart = time.time()

                        cur.executemany(sqlBulkMerge, dbBuffer)
                        conn.commit()

                        timeDBend = time.time()
                        dbWriteDuration = timeDBend - timeDBstart
                        # ----------------------------------------------


                        dbBuffer.clear()
                        currentBatchSizeByte = 0

                        gcCounter += 1
                        if gcCounter >= GCINTERVAL:
                            gc.collect()
                            gcCounter = 0

                        # ----------------------------------------------
                        # Metrics Collection
                        # ----------------------------------------------
                        currentTime = time.time()
                        elapsed = currentTime - startTime
                        speed = completedFilesCount / elapsed if elapsed > 0 else 0
                        remainingFiles = totalFilesCount - completedFilesCount

                        # Calculate Context Switches Rate
                        currentCTXSwitch = psutil.cpu_stats().ctx_switches
                        timeDiff = currentTime - lastCheckTime
                        ctxRate = 0
                        if timeDiff > 0:
                            ctxRate = (currentCTXSwitch - lastCTXSwitchCount) / timeDiff

                        # Update pointers
                        lastCTXSwitchCount = currentCTXSwitch
                        lastCheckTime = currentTime

                        # Append to Lists
                        statsTimeStamps.append(elapsed)
                        statsThroughput.append(speed)
                        statsRemaining.append(remainingFiles)
                        statsCpu.append(psutil.cpu_percent())
                        statsRam.append(psutil.virtual_memory().percent)
                        statsDbWait.append(dbWriteDuration)
                        statsCtxSwitches.append(ctxRate)

                        totalProcessed = completedFilesCount + skippedCount
                        print(
                            f"    Progress: {totalProcessed}/{totalFilesCount} Success: {completedFilesCount} | Skipped: {skippedCount} | Speed: {speed:.2f}/s",
                            end="\r")

                    except Exception as e:
                        print(f"\n[DB Error] {e}")
                        conn.rollback()

    if dbBuffer:
        try:
            print(f"\nStatus: Writing final batch of {len(dbBuffer)} records...")
            cur.executemany(sqlBulkMerge, dbBuffer)
            conn.commit()
        except Exception as e:
            print(f"\n[DB Error] Final Batch: {e}")

    conn.close()

    totalTime = time.time() - startTime
    print(f"\n\n[Final] Job Done! Processed {completedFilesCount} Files in {totalTime:.2f} seconds.")

    try:
        return (statsTimeStamps, statsThroughput, statsCpu, statsRam, statsRemaining, statsDbWait,
                statsCtxSwitches, totalFilesCount)
    except Exception as e:
        print(f"Graph Error: {e}")

if __name__ == "__main__":
    print("--- STARTING ETL PROCESS ---")
    print(f"START {time.time()}")
    metricsFactBulkData = fetchFactBulkData()
    statsTimeStamps, statsThroughput, statsCpu, statsRam, statsRemaining, statsDbWait, statsCtxSwitches, totalTasks = metricsFactBulkData
    generatePerformanceReport(statsTimeStamps, statsThroughput, statsCpu, statsRam, statsRemaining, statsDbWait,
                              statsCtxSwitches, totalTasks, "companyFact_Final_Metrics", unit_label="CIKs")
    print("\n" + "=" * 50 + "\n")
    metricsSubmissionsBulkData = fetchSubmissionsBulkData()
    statsTimeStamps, statsThroughput, statsCpu, statsRam, statsRemaining, statsDbWait, statsCtxSwitches, totalTasks = metricsSubmissionsBulkData
    generatePerformanceReport(statsTimeStamps, statsThroughput, statsCpu, statsRam, statsRemaining, statsDbWait,
                              statsCtxSwitches, totalTasks, "Submissions_Final_Metrics", unit_label="Files")
    print("\n--- ALL TASKS COMPLETED ---")