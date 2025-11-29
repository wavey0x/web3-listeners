import os
import time
import logging
import threading
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Import the main functions from all scripts
from data_fetchers.rsup_incentives import main as incentives_main
from data_fetchers.yb_incentives import main as yb_incentives_main
from data_fetchers.resupply_dao import main as dao_main
from data_fetchers.resupply_retention import main as retention_main

def run_incentives():
    """Run the RSUP incentives monitoring service"""
    while True:
        try:
            logger.info("Starting RSUP incentives service...")
            incentives_main()
        except Exception as e:
            logger.error(f"Error in RSUP incentives service: {str(e)}")
            time.sleep(60)

def run_yb_incentives():
    """Run the YB incentives monitoring service"""
    while True:
        try:
            print("DEBUG: YB incentives thread started")
            logger.info("Starting YB incentives service...")
            yb_incentives_main()
        except Exception as e:
            import traceback
            error_msg = f"Error in YB incentives service: {str(e)}\n{traceback.format_exc()}"
            print(f"DEBUG: {error_msg}")
            logger.error(error_msg)
            time.sleep(60)

def run_dao():
    """Run the Resupply DAO monitoring service"""
    while True:
        try:
            logger.info("Starting Resupply DAO service...")
            dao_main()
        except Exception as e:
            logger.error(f"Error in Resupply DAO service: {str(e)}")
            time.sleep(60)

def run_retention():
    """Run the Resupply Retention monitoring service"""
    while True:
        try:
            logger.info("Starting Resupply Retention service...")
            retention_main()
        except Exception as e:
            logger.error(f"Error in Resupply Retention service: {str(e)}")
            time.sleep(60)

def main():
    """Main entry point that runs all services in separate threads"""
    # Load environment variables
    load_dotenv()
    
    # Create and start threads for each service
    incentives_thread = threading.Thread(target=run_incentives, name="RSUP-Incentives")
    yb_incentives_thread = threading.Thread(target=run_yb_incentives, name="YB-Incentives")
    dao_thread = threading.Thread(target=run_dao, name="Resupply-DAO")
    retention_thread = threading.Thread(target=run_retention, name="Resupply-Retention")

    # Set threads as daemon threads so they exit when main thread exits
    incentives_thread.daemon = True
    yb_incentives_thread.daemon = True
    dao_thread.daemon = True
    retention_thread.daemon = True

    # Start the threads
    incentives_thread.start()
    yb_incentives_thread.start()
    dao_thread.start()
    retention_thread.start()

    logger.info("All four services started successfully")
    
    try:
        # Keep the main thread alive and monitor the services
        while True:
            if not incentives_thread.is_alive():
                logger.error("RSUP incentives service died, restarting...")
                incentives_thread = threading.Thread(target=run_incentives, name="RSUP-Incentives")
                incentives_thread.daemon = True
                incentives_thread.start()

            if not yb_incentives_thread.is_alive():
                logger.error("YB incentives service died, restarting...")
                yb_incentives_thread = threading.Thread(target=run_yb_incentives, name="YB-Incentives")
                yb_incentives_thread.daemon = True
                yb_incentives_thread.start()

            if not dao_thread.is_alive():
                logger.error("Resupply DAO service died, restarting...")
                dao_thread = threading.Thread(target=run_dao, name="Resupply-DAO")
                dao_thread.daemon = True
                dao_thread.start()

            if not retention_thread.is_alive():
                logger.error("Resupply Retention service died, restarting...")
                retention_thread = threading.Thread(target=run_retention, name="Resupply-Retention")
                retention_thread.daemon = True
                retention_thread.start()

            time.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        logger.info("Shutting down services...")
        # The daemon threads will be terminated automatically

if __name__ == '__main__':
    main() 
