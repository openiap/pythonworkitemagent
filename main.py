import os, json
from openiap import Client, ClientError
import time
import asyncio
from functools import partial

defaultwiq = "default_queue"
queue_task = None
main_loop = None  # Store the main event loop
original_files = []
working = False

def lstat():
    """Get list of files in current directory"""
    try:
        files = [f for f in os.listdir(".") if os.path.isfile(f)]
        return files
    except Exception:
        return []

def cleanup_files(original_files):
    """Remove files that were created during processing"""
    try:
        current_files = lstat()
        files_to_delete = [f for f in current_files if f not in original_files]
        for file in files_to_delete:
            try:
                os.unlink(file)
            except Exception:
                pass
    except Exception:
        pass

async def process_workitem(workitem):
    """Process a single workitem"""
    client.info(f"Processing workitem id {workitem['id']}, retry #{workitem.get('retries', 0)}")

    
    # if payload is a string parse it as json else assign it a new dict
    payload = workitem.get('payload')
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            payload = {}
    elif not isinstance(payload, dict):
        payload = {}
    payload['name'] = "kitty"    
    # Update workitem properties
    workitem['name'] = "Hello kitty"
    
    # Write file as example
    with open("hello.txt", "w") as f:
        f.write("Hello kitty")
    
    # Simulate async processing
    await asyncio.sleep(2)
    # convert workitem payload back to string
    workitem['payload'] = json.dumps(payload)
    return workitem

async def process_workitem_wrapper(original_files, workitem):
    """Wrapper to handle workitem processing with error handling"""
    try:
        await process_workitem(workitem)
        workitem["state"] = "successful"
    except Exception as error:
        workitem["state"] = "retry"
        workitem["errortype"] = "application"  # Retryable error
        workitem["errormessage"] = str(error)
        workitem["errorsource"] = str(error)
        client.error(str(error))
    
    current_files = lstat()
    files_add = [f for f in current_files if f not in original_files]
    if files_add:
        client.update_workitem(workitem, files=files_add)
    else:
        client.update_workitem(workitem)

async def on_queue_message():
    """Handle queue message - process all available workitems"""
    global working
    if working:
        return
    
    try:
        wiq = os.environ.get("wiq") or os.environ.get("SF_AMQPQUEUE") or defaultwiq
        queue = os.environ.get("queue") or wiq
        working = True
        workitem = None
        counter = 0
        
        while True:
            workitem = client.pop_workitem(wiq=wiq)
            if workitem is None:
                break
            counter += 1
            await process_workitem_wrapper(original_files, workitem)
            cleanup_files(original_files)
        
        if counter > 0:
            client.info(f"No more workitems in {wiq} workitem queue")
        
        if os.environ.get("SF_VMID"):
            client.info(f"Exiting application as running in serverless VM {os.environ.get('SF_VMID')}")
            os._exit(0)
            
    except Exception as error:
        client.error(str(error))
    finally:
        cleanup_files(original_files)
        working = False

def schedule_coroutine(coro):
    """Thread-safe way to schedule a coroutine on the main event loop"""
    global main_loop
    try:
        if main_loop and main_loop.is_running():
            return asyncio.run_coroutine_threadsafe(coro, main_loop)
        else:
            client.warn("Main event loop not available, cannot schedule coroutine")
            # Close the coroutine to prevent the warning
            coro.close()
            return None
    except Exception as e:
        client.error(f"Error scheduling coroutine: {e}")
        # Close the coroutine to prevent the warning
        coro.close()
        return None

def handle_queue(event, counter):
    """Handle queue message - only called when new workitems are available"""
    client.info(f"Queue event #{counter} Received")
    try:
        # Process workitems when notified
        future = schedule_coroutine(on_queue_message())
        if future:
            future.add_done_callback(lambda f: f.exception() if f.exception() else None)
    except Exception as e:
        client.error(f"Error in queue handler: {e}")

async def on_connected():
    """Handle connection event"""
    try:
        wiq = os.environ.get("wiq") or os.environ.get("SF_AMQPQUEUE") or defaultwiq
        queue = os.environ.get("queue") or wiq
        queuename = client.register_queue(queuename=queue, callback=handle_queue)
        client.info(f"Consuming message queue: {queuename}")
        
        if os.environ.get("SF_VMID"):
            await on_queue_message()
    except Exception as error:
        client.error(str(error))
        os._exit(0)

def onclientevent(result, counter):
    event = result.get("event")
    reason = result.get("reason")
    if event == "SignedIn":
        # Schedule the async on_connected function
        try:
            future = schedule_coroutine(on_connected())
            if future:
                future.add_done_callback(lambda f: client.error(str(f.exception())) if f.exception() else None)
        except Exception as e:
            client.error(f"Error scheduling on_connected: {e}")
    if event == "Disconnected":
        client.info("Disconnected from server")

async def main():
    global original_files, main_loop, client
    
    try:
        original_files = lstat()
        client = Client()
        client.enable_tracing("openiap=info", "")
        client.connect()
        
        eventid = client.on_client_event(callback=onclientevent)
        client.info(f"Client event registered with id: {eventid}")
        
        main_loop = asyncio.get_event_loop()
        
        # Keep the event loop running
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            client.info("Shutting down...")
            
    except ClientError as e:
        client.error(f"An error occurred: {e}")
    except Exception as e:
        client.error(f"An error occurred: {e}")
    finally:
        if queue_task:
            queue_task.cancel()
        client.free()

if __name__ == "__main__":
    WIQ = os.environ.get("wiq") or os.environ.get("SF_AMQPQUEUE") or defaultwiq
    if not WIQ:
        raise ValueError("Workitem queue name (wiq) is required")
    
    # Initialize client as global variable
    client = None
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        if client:
            client.info("Shutting down...")
    finally:
        if client:
            client.free()
