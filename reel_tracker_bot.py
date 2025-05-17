import os
import telebot
import requests
import json
from moviepy.editor import VideoFileClip, TextClip, CompositeVideoClip
from pytube import YouTube
import tempfile
import shutil
from dotenv import load_dotenv
import re

# Load environment variables
load_dotenv()

# Initialize bot with your token
BOT_TOKEN = os.getenv('BOT_TOKEN')
ENSEMBLE_TOKEN = os.getenv('ENSEMBLE_TOKEN')
WATERMARK_TEXT = os.getenv('WATERMARK_TEXT', 'Downloaded with ReelBot')

bot = telebot.TeleBot(BOT_TOKEN)

def get_instagram_reel_data(reel_url):
    """Get reel data from EnsembleData API"""
    try:
        # Extract user_id from the URL
        # Example URL format: https://www.instagram.com/reel/CODE/
        # or https://www.instagram.com/username/reel/CODE/
        match = re.search(r'instagram\.com/(?:([^/]+)/)?reel/([^/?#&]+)', reel_url)
        if not match:
            raise Exception("Invalid Instagram reel URL")
            
        username, shortcode = match.groups()
        
        # Make API request to EnsembleData
        api_url = 'https://ensembledata.com/apis/instagram/user/reels'
        params = {
            'user_id': username,  # Using username instead of numeric ID
            'depth': 1,
            'include_feed_video': True,
            'token': ENSEMBLE_TOKEN
        }
        
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        if not data.get('data', {}).get('reels'):
            raise Exception("No reels found")
            
        # Find the specific reel by shortcode
        reels = data['data']['reels']
        target_reel = None
        for reel in reels:
            if reel['media']['code'] == shortcode:
                target_reel = reel['media']
                break
                
        if not target_reel:
            raise Exception("Reel not found")
        
        # Extract video URL
        video_url = target_reel['video_versions'][0]['url']
        
        return {
            'video_url': video_url,
            'view_count': target_reel.get('view_count', 0),
            'play_count': target_reel.get('play_count', 0),
            'like_count': target_reel.get('like_count', 0),
            'comment_count': target_reel.get('comment_count', 0)
        }
    except Exception as e:
        raise Exception(f"Error fetching reel data: {str(e)}")

def process_video(video_path, output_path):
    """Process video to remove audio and add watermark"""
    try:
        # Load video
        video = VideoFileClip(video_path)
        
        # Create watermark text
        txt_clip = TextClip(WATERMARK_TEXT, fontsize=30, color='white', font='Arial')
        txt_clip = txt_clip.set_position(('center', 'bottom')).set_duration(video.duration)
        
        # Combine video and watermark
        final_video = CompositeVideoClip([video, txt_clip])
        
        # Write final video without audio
        final_video.write_videofile(output_path, codec='libx264', audio=False)
        
        # Close clips
        video.close()
        final_video.close()
        
        return output_path
    except Exception as e:
        raise Exception(f"Error processing video: {str(e)}")

def download_video(url, output_path):
    """Download video from URL"""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        return output_path
    except Exception as e:
        raise Exception(f"Error downloading video: {str(e)}")

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.reply_to(message, "Welcome! Send me an Instagram reel link to get started.")

@bot.message_handler(func=lambda message: True)
def handle_message(message):
    try:
        # Create temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Get reel data
            reel_data = get_instagram_reel_data(message.text)
            
            # Download video
            video_path = os.path.join(temp_dir, 'video.mp4')
            download_video(reel_data['video_url'], video_path)
            
            # Process video
            processed_path = os.path.join(temp_dir, 'processed.mp4')
            process_video(video_path, processed_path)
            
            # Send video with stats
            stats_message = f"📊 Reel Stats:\nViews: {reel_data['view_count']}\nPlays: {reel_data['play_count']}\nLikes: {reel_data['like_count']}\nComments: {reel_data['comment_count']}"
            
            with open(processed_path, 'rb') as video:
                bot.send_video(message.chat.id, video, caption=stats_message)
                
    except Exception as e:
        bot.reply_to(message, f"Error: {str(e)}")

# Start the bot
if __name__ == "__main__":
    print("Bot started...")
    bot.polling(none_stop=True) 
