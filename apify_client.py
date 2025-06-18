import os
import aiohttp
import asyncio
import logging
from typing import List, Dict, Optional, Union
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class ApifyTaskManager:
    """Manages Apify tasks for Instagram scraping"""
    
    def __init__(self, apify_client):
        self.client = apify_client
        self.active_tasks = {}  # task_id -> task_info
        self.task_queue = []    # Queue for pending tasks
        self.max_concurrent_tasks = int(os.getenv("MAX_CONCURRENT_TASKS", "3"))
        
    async def create_task(self, urls: List[str], task_type: str = "single", priority: int = 1) -> str:
        """Create a new Apify task for scraping URLs"""
        try:
            task_id = f"{task_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{len(urls)}"
            
            # Prepare actor input
            actor_input = {
                "startUrls": [{"url": url} for url in urls],
                "maxConcurrency": min(len(urls), 5),
                "requestDelay": 3,
                "maxRetries": 2,
                "enableDebugLogs": False
            }
            
            # Add cookie configuration if available
            if self.client.cookie_sets:
                actor_input["cookieSets"] = self.client.cookie_sets
            
            task_info = {
                "task_id": task_id,
                "urls": urls,
                "task_type": task_type,
                "priority": priority,
                "status": "pending",
                "created_at": datetime.now(),
                "actor_input": actor_input,
                "run_id": None,
                "results": None,
                "error": None
            }
            
            # Add to queue
            self.task_queue.append(task_info)
            self.task_queue.sort(key=lambda x: x["priority"], reverse=True)
            
            logger.info(f"📋 Created task {task_id} for {len(urls)} URLs (type: {task_type})")
            return task_id
            
        except Exception as e:
            logger.error(f"❌ Error creating task: {str(e)}")
            raise Exception(f"Failed to create task: {str(e)}")
    
    async def process_task_queue(self):
        """Process pending tasks from the queue"""
        while self.task_queue and len(self.active_tasks) < self.max_concurrent_tasks:
            task_info = self.task_queue.pop(0)
            await self._start_task(task_info)
    
    async def _start_task(self, task_info: Dict):
        """Start executing a task"""
        try:
            task_id = task_info["task_id"]
            logger.info(f"🚀 Starting task {task_id}")
            
            async with aiohttp.ClientSession() as session:
                # Start actor run
                async with session.post(
                    f"{self.client.base_url}/acts/{self.client.actor_id}/runs",
                    json=task_info["actor_input"],
                    headers={
                        "Authorization": f"Bearer {self.client.token}",
                        "Content-Type": "application/json"
                    }
                ) as response:
                    if response.status != 201:
                        raise Exception(f"Failed to start Apify run: {response.status}")
                    
                    run_data = await response.json()
                    run_info = run_data["data"]
                    
                    task_info["run_id"] = run_info["id"]
                    task_info["status"] = "running"
                    task_info["started_at"] = datetime.now()
                    
                    self.active_tasks[task_id] = task_info
                    
                    logger.info(f"✅ Task {task_id} started with run ID: {run_info['id']}")
                    
        except Exception as e:
            task_info["status"] = "failed"
            task_info["error"] = str(e)
            logger.error(f"❌ Failed to start task {task_id}: {str(e)}")
    
    async def check_task_status(self, task_id: str) -> Dict:
        """Check the status of a specific task"""
        if task_id not in self.active_tasks:
            # Check if it's in queue
            for task in self.task_queue:
                if task["task_id"] == task_id:
                    return {"status": "queued", "task_info": task}
            return {"status": "not_found"}
        
        task_info = self.active_tasks[task_id]
        
        if task_info["status"] == "running":
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"{self.client.base_url}/actor-runs/{task_info['run_id']}",
                        headers={"Authorization": f"Bearer {self.client.token}"}
                    ) as response:
                        if response.status == 200:
                            run_data = await response.json()
                            run_status = run_data["data"]["status"]
                            
                            if run_status == "SUCCEEDED":
                                # Get results
                                results = await self._get_task_results(task_info)
                                task_info["results"] = results
                                task_info["status"] = "completed"
                                task_info["completed_at"] = datetime.now()
                                
                                # Remove from active tasks
                                del self.active_tasks[task_id]
                                
                                logger.info(f"✅ Task {task_id} completed successfully")
                                
                            elif run_status == "FAILED":
                                task_info["status"] = "failed"
                                task_info["error"] = "Apify run failed"
                                task_info["completed_at"] = datetime.now()
                                
                                # Remove from active tasks
                                del self.active_tasks[task_id]
                                
                                logger.error(f"❌ Task {task_id} failed")
                                
            except Exception as e:
                logger.error(f"❌ Error checking task {task_id}: {str(e)}")
        
        return {"status": task_info["status"], "task_info": task_info}
    
    async def _get_task_results(self, task_info: Dict) -> List[Dict]:
        """Get results from a completed task"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.client.base_url}/actor-runs/{task_info['run_id']}/dataset/items",
                    headers={"Authorization": f"Bearer {self.client.token}"},
                    params={"format": "json"}
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        raise Exception(f"Failed to get results: {response.status}")
        except Exception as e:
            logger.error(f"❌ Error getting results for task {task_info['task_id']}: {str(e)}")
            return []
    
    async def wait_for_task(self, task_id: str, timeout: int = 300) -> Dict:
        """Wait for a task to complete with timeout"""
        start_time = datetime.now()
        
        while (datetime.now() - start_time).seconds < timeout:
            status_info = await self.check_task_status(task_id)
            
            if status_info["status"] in ["completed", "failed"]:
                return status_info
            
            await asyncio.sleep(10)  # Check every 10 seconds
        
        return {"status": "timeout", "task_info": None}
    
    async def get_active_tasks(self) -> Dict:
        """Get information about all active tasks"""
        return {
            "active_tasks": len(self.active_tasks),
            "queued_tasks": len(self.task_queue),
            "tasks": list(self.active_tasks.keys())
        }
    
    async def cleanup_old_tasks(self, max_age_hours: int = 24):
        """Clean up old completed/failed tasks from memory"""
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        
        # This would typically involve database cleanup in a production system
        logger.info(f"🧹 Cleaned up tasks older than {max_age_hours} hours")

class ApifyClient:
    def __init__(self):
        self.token = os.getenv("APIFY_TOKEN")
        self.actor_id = os.getenv("APIFY_ACTOR_ID", "0rKQGAkScID5LvXfC")
        self.base_url = "https://api.apify.com/v2"
        
        if not self.token:
            raise ValueError("APIFY_TOKEN is required in environment variables")
        
        # Setup cookie sets from environment
        self.cookie_sets = self._setup_cookie_sets()
        
        # Initialize task manager
        self.task_manager = ApifyTaskManager(self)
        
        # Start background task processor
        asyncio.create_task(self._background_task_processor())
        
    async def _background_task_processor(self):
        """Background task to process the task queue"""
        while True:
            try:
                await self.task_manager.process_task_queue()
                
                # Check status of active tasks
                for task_id in list(self.task_manager.active_tasks.keys()):
                    await self.task_manager.check_task_status(task_id)
                
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"❌ Error in background task processor: {str(e)}")
                await asyncio.sleep(60)  # Wait longer on error
    
    def _setup_cookie_sets(self) -> List[Dict]:
        """Setup Instagram cookie sets from environment variables"""
        # Try to get multiple cookie sets from environment
        cookie_sets_env = os.getenv("COOKIE_SETS")
        if cookie_sets_env:
            try:
                import json
                return json.loads(cookie_sets_env)
            except json.JSONDecodeError:
                logger.warning("⚠️ Invalid COOKIE_SETS format, using single cookie set")
        
        # Fallback to single cookie set
        single_cookie_set = {
            "sessionid": os.getenv("INSTAGRAM_SESSION_ID"),
            "csrftoken": os.getenv("INSTAGRAM_CSRF_TOKEN"),
            "ds_user_id": os.getenv("INSTAGRAM_USER_ID"),
            "mid": os.getenv("INSTAGRAM_MID"),
            "ig_did": os.getenv("INSTAGRAM_IG_DID"),
            "datr": os.getenv("INSTAGRAM_DATR")
        }
        
        # Check if we have required cookies
        if not all([single_cookie_set["sessionid"], single_cookie_set["csrftoken"], single_cookie_set["ds_user_id"]]):
            logger.warning("⚠️ Missing required Instagram cookies. Scraping may fail for private content")
            return []
        
        return [single_cookie_set]
    
    def _validate_instagram_url(self, url: str) -> bool:
        """Validate if URL is a valid Instagram URL"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return (parsed.hostname and 'instagram.com' in parsed.hostname and 
                   any(path in url for path in ['/p/', '/reel/', '/tv/']))
        except:
            return False
    
    def _extract_shortcode(self, url: str) -> Optional[str]:
        """Extract shortcode from Instagram URL"""
        import re
        patterns = [
            r'instagram\.com/(?:[^/]+/)?(?:p|reel|tv)/([A-Za-z0-9_-]+)',
            r'/(?:p|reel|tv)/([A-Za-z0-9_-]+)',
            r'^([A-Za-z0-9_-]+)$'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None
    
    async def create_scraping_task(self, urls: Union[str, List[str]], task_type: str = "single") -> str:
        """Create a new scraping task (async, non-blocking)"""
        if isinstance(urls, str):
            urls = [urls]
        
        # Validate URLs
        valid_urls = []
        for url in urls:
            if not url.startswith('http'):
                shortcode = self._extract_shortcode(url)
                if shortcode:
                    url = f"https://www.instagram.com/reel/{shortcode}/"
            
            if self._validate_instagram_url(url) or self._extract_shortcode(url):
                valid_urls.append(url)
            else:
                logger.warning(f"⚠️ Invalid URL skipped: {url}")
        
        if not valid_urls:
            raise ValueError("No valid Instagram URLs provided")
        
        # Determine priority based on task type
        priority = 3 if task_type == "single" else 1  # Single submissions get higher priority
        
        return await self.task_manager.create_task(valid_urls, task_type, priority)
    
    async def get_task_results(self, task_id: str, wait: bool = False, timeout: int = 300) -> Dict:
        """Get results from a task"""
        if wait:
            result = await self.task_manager.wait_for_task(task_id, timeout)
        else:
            result = await self.task_manager.check_task_status(task_id)
        
        if result["status"] == "completed" and result["task_info"]["results"]:
            # Convert to legacy format for compatibility
            processed_results = []
            for item in result["task_info"]["results"]:
                processed_results.append({
                    "url": item.get("url", ""),
                    "username": item.get("username", ""),
                    "type": item.get("type", "post"),
                    "views": item.get("views", 0),
                    "likes": item.get("likes", 0),
                    "comments": item.get("comments", 0),
                    "caption": item.get("caption", ""),
                    "timestamp": item.get("timestamp"),
                    "media_url": item.get("mediaUrl", ""),
                    "success": True
                })
            
            return {
                "status": "completed",
                "results": processed_results,
                "task_info": result["task_info"]
            }
        
        return result
    
    async def scrape_instagram_post(self, url: str) -> Dict:
        """Scrape a single Instagram post (blocking method for compatibility)"""
        try:
            # Create task and wait for completion
            task_id = await self.create_scraping_task([url], "single")
            result = await self.get_task_results(task_id, wait=True, timeout=300)
            
            if result["status"] == "completed" and result["results"]:
                return result["results"][0]
            else:
                raise Exception(f"Task failed: {result.get('task_info', {}).get('error', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"❌ Error scraping Instagram post {url}: {str(e)}")
            raise Exception(f"Failed to scrape Instagram post: {str(e)}")
    
    async def batch_scrape_posts(self, urls: List[str], batch_size: int = 10) -> List[Dict]:
        """Scrape multiple Instagram posts using task system"""
        results = []
        
        # Create tasks for batches
        task_ids = []
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            task_id = await self.create_scraping_task(batch, "batch")
            task_ids.append(task_id)
        
        # Wait for all tasks to complete
        for task_id in task_ids:
            result = await self.get_task_results(task_id, wait=True, timeout=600)
            
            if result["status"] == "completed":
                results.extend(result["results"])
            else:
                # Add failed results
                for url in result.get("task_info", {}).get("urls", []):
                    results.append({
                        "url": url,
                        "success": False,
                        "error": result.get("task_info", {}).get("error", "Task failed")
                    })
        
        return results
    
    async def get_reel_data(self, shortcode_or_url: str) -> Dict:
        """
        Legacy method to maintain compatibility with existing reel tracker code
        Returns data in the format expected by the existing system
        """
        try:
            result = await self.scrape_instagram_post(shortcode_or_url)
            
            # Convert to legacy format
            return {
                "owner_username": result["username"],
                "view_count": result["views"],
                "play_count": result["views"],  # For compatibility
                "taken_at_timestamp": result["timestamp"]
            }
            
        except Exception as e:
            logger.error(f"Error in get_reel_data: {str(e)}")
            raise Exception(f"Error fetching reel data: {str(e)}")
    
    async def get_task_status(self) -> Dict:
        """Get overall task system status"""
        return await self.task_manager.get_active_tasks()

# Global instance for backward compatibility
apify_client = None

def get_apify_client() -> ApifyClient:
    """Get or create global Apify client instance"""
    global apify_client
    if apify_client is None:
        apify_client = ApifyClient()
    return apify_client 
