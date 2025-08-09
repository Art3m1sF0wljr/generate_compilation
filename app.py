import os
import time
import logging
import random
from datetime import datetime
import googleapiclient.discovery
import yt_dlp
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.exceptions import RefreshError
from googleapiclient.errors import HttpError
import subprocess

# Configuration
CLIENT_SECRETS_FILE = "client_secrets_1.json"
SCOPES = ["https://www.googleapis.com/auth/youtube.upload",
          "https://www.googleapis.com/auth/youtube.force-ssl",
          "https://www.googleapis.com/auth/youtube.readonly"]
API_SERVICE_NAME = "youtube"
API_VERSION = "v3"
TOKEN_FILE = "token.json"
DOWNLOAD_DIR = "./downloads"
OUTPUT_DIR = "./output"
FFMPEG_PATH = "ffmpeg"  # Adjust if ffmpeg is not in PATH
LOFI_AUDIO_FILE = "lofi.mp3"  # Make sure this file exists in your directory

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('playlist_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def cleanup_files():
    """Delete all downloaded and generated files"""
    try:
        # Delete downloaded files
        for filename in os.listdir(DOWNLOAD_DIR):
            file_path = os.path.join(DOWNLOAD_DIR, filename)
            if os.path.isfile(file_path):
                os.unlink(file_path)
        
        # Delete output files (keep the final uploaded versions)
        for filename in os.listdir(OUTPUT_DIR):
            if not filename.endswith('_uploaded.mp4'):
                file_path = os.path.join(OUTPUT_DIR, filename)
                if os.path.isfile(file_path):
                    os.unlink(file_path)
        
        logger.info("Cleanup complete - deleted all temporary files")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")


def create_spedup_version(original_video_path):
    """Create a 4x speed version with random LOFI audio, matching video duration exactly"""
    if not os.path.exists(original_video_path):
        logger.error(f"Original video not found: {original_video_path}")
        return None
    
    if not os.path.exists(LOFI_AUDIO_FILE):
        logger.error(f"LOFI audio file not found: {LOFI_AUDIO_FILE}")
        return None

    # Get duration of original video (after speedup)
    try:
        # Get original video duration
        cmd = [FFMPEG_PATH, '-i', original_video_path]
        result = subprocess.run(cmd, stderr=subprocess.PIPE, text=True)
        duration_line = [line for line in result.stderr.split('\n') if 'Duration:' in line][0]
        original_duration = duration_line.split('Duration: ')[1].split(',')[0]
        h, m, s = original_duration.split(':')
        original_seconds = float(h) * 3600 + float(m) * 60 + float(s)
        spedup_duration = original_seconds / 4  # 4x speed
        
        # Get duration of LOFI audio
        cmd = [FFMPEG_PATH, '-i', LOFI_AUDIO_FILE]
        result = subprocess.run(cmd, stderr=subprocess.PIPE, text=True)
        duration_line = [line for line in result.stderr.split('\n') if 'Duration:' in line][0]
        lofi_duration = duration_line.split('Duration: ')[1].split(',')[0]
        h, m, s = lofi_duration.split(':')
        lofi_seconds = float(h) * 3600 + float(m) * 60 + float(s)
    except Exception as e:
        logger.error(f"Could not get durations: {e}")
        return None

    # Calculate random start point ensuring audio is long enough
    max_start = lofi_seconds - spedup_duration - 1  # 1 second buffer
    if max_start <= 0:
        logger.error("LOFI audio is too short for the sped-up video")
        return None
    
    random_start = random.uniform(0, max_start)
    
    # Create output filename
    base_name = os.path.splitext(os.path.basename(original_video_path))[0]
    output_path = os.path.join(OUTPUT_DIR, f"{base_name}_4x_lofi.mp4")

    # FFmpeg command to create 4x speed version with exactly matching duration
    cmd = [
        FFMPEG_PATH,
        '-i', original_video_path,
        '-i', LOFI_AUDIO_FILE,
        '-filter_complex',
        f'[0:v]setpts=0.35*PTS,trim=duration={spedup_duration}[v];'  # Force exact duration
        f'[1:a]atrim=start={random_start},asetpts=PTS-STARTPTS,adelay=0|0,apad=whole_dur={spedup_duration}[a]',  # Pad audio to match
        '-map', '[v]',
        '-map', '[a]',
        '-shortest',  # Shouldn't be needed but kept as safety
        '-c:v', 'libx264',
        '-preset', 'slower',
        '-crf', '18',
        '-c:a', 'aac',
        '-b:a', '192k',
        '-t', str(spedup_duration),  # Explicit duration limit
        output_path
    ]

    logger.info(f"Creating 4x speed version (duration: {spedup_duration:.2f}s) with LOFI audio...")
    try:
        subprocess.run(cmd, check=True)
        
        return output_path
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to create 4x version: {e}")
        return None

def get_authenticated_service():
    """Authenticate and return the YouTube service, caching credentials"""
    creds = None

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid or not creds.has_scopes(SCOPES):
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except RefreshError as e:
                logger.error(f"Failed to refresh token: {e}")
                os.remove(TOKEN_FILE)
                return get_authenticated_service()
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())

    return build(API_SERVICE_NAME, API_VERSION, credentials=creds)

def get_playlist_videos(youtube, playlist_id):
    """Get all videos from a playlist including private/unlisted"""
    videos = []
    next_page_token = None
    
    try:
        while True:
            request = youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=playlist_id,
                maxResults=100,
                pageToken=next_page_token
            )
            response = request.execute()
            
            for item in response['items']:
                video_id = item['contentDetails']['videoId']
                published_at = item['snippet']['publishedAt']
                title = item['snippet']['title']
                
                # Get video details to check privacy status
                video_request = youtube.videos().list(
                    part="status",
                    id=video_id
                )
                video_response = video_request.execute()
                
                if video_response['items']:
                    privacy_status = video_response['items'][0]['status']['privacyStatus']
                    videos.append({
                        'id': video_id,
                        'published_at': published_at,
                        'title': title,
                        'privacy_status': privacy_status
                    })
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
                
    except HttpError as e:
        logger.error(f"YouTube API error: {e}")
    except Exception as e:
        logger.error(f"Error fetching playlist videos: {e}")
    
    return videos

def filter_august_2025_videos(videos):
    """Filter videos published in August 2025"""
    august_2025_videos = []
    
    for video in videos:
        try:
            published_date = datetime.strptime(video['published_at'], '%Y-%m-%dT%H:%M:%SZ')
            if published_date.year == 2025 and published_date.month == 7:
                august_2025_videos.append(video)
        except Exception as e:
            logger.error(f"Error parsing date for video {video['id']}: {e}")
    
    # Sort by publication date
    august_2025_videos.sort(key=lambda x: x['published_at'])
    
    return august_2025_videos

def download_video(video_id, filename):
    """Download video using yt-dlp"""
    url = f"https://www.youtube.com/watch?v={video_id}"
    ydl_opts = {
        'outtmpl': os.path.join(DOWNLOAD_DIR, filename),
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        return True
    except Exception as e:
        logger.error(f"Download failed: {e}")
        return False

def combine_videos(video_files, output_filename):
    """Combine multiple videos into one using ffmpeg, upscaling to highest resolution"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    
    # First pass: Detect highest resolution (same as before)
    max_width = 0
    max_height = 0
    for video_file in video_files:
        try:
            cmd = [
                FFMPEG_PATH,
                '-i', os.path.join(DOWNLOAD_DIR, video_file),
                '-f', 'null', '-'
            ]
            result = subprocess.run(cmd, stderr=subprocess.PIPE, text=True)
            stream_info = [line for line in result.stderr.split('\n') if 'Stream #0:0' in line and 'Video:' in line]
            if stream_info:
                parts = stream_info[0].split(',')
                resolution_part = [p for p in parts if any(x in p for x in [' hd', ' sd', 'x', '0x0'])][0].strip()
                resolution = resolution_part.split(' ')[0]
                
                if 'x' in resolution:
                    width, height = resolution.split('x')
                    width = ''.join(filter(str.isdigit, width))
                    height = ''.join(filter(str.isdigit, height.split(' ')[0]))
                    
                    if width and height:
                        width = int(width)
                        height = int(height)
                        if width > max_width:
                            max_width = width
                            max_height = height
        except Exception as e:
            logger.error(f"Error detecting resolution for {video_file}: {e}")
            continue
    
    if max_width == 0 or max_height == 0:
        max_width = 1280
        max_height = 720
        logger.warning(f"Using fallback resolution: {max_width}x{max_height}")
    
    logger.info(f"Upscaling all videos to: {max_width}x{max_height}")
    
    # Create intermediate files with upscaling
    intermediate_dir = os.path.join(OUTPUT_DIR, "intermediate")
    os.makedirs(intermediate_dir, exist_ok=True)
    intermediate_files = []
    
    for idx, video_file in enumerate(video_files):
        intermediate_path = os.path.join(intermediate_dir, f"int_{idx}.mp4")
        cmd = [
            FFMPEG_PATH,
            '-i', os.path.join(DOWNLOAD_DIR, video_file),
            '-vf', f'scale={max_width}:{max_height}:flags=lanczos',
            '-c:v', 'libx264',
            '-preset', 'slower',
            '-crf', '18',
            '-c:a', 'aac',
            '-b:a', '192k',
            '-movflags', '+faststart',
            intermediate_path
        ]
        try:
            logger.info(f"Upscaling {video_file}...")
            subprocess.run(cmd, check=True)
            intermediate_files.append(intermediate_path)
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to process {video_file}: {e}")
            continue
    
    if not intermediate_files:
        logger.error("No valid intermediate files created")
        return None
    
    # Concatenate the processed files
    try:
        # Create concatenation list with absolute paths
        list_path = os.path.join(intermediate_dir, "concat_list.txt")
        with open(list_path, 'w') as f:
            for file_path in intermediate_files:
                # Write absolute paths to avoid any relative path confusion
                abs_path = os.path.abspath(file_path)
                f.write(f"file '{abs_path}'\n")
        
        # Verify the list file was created correctly
        if not os.path.exists(list_path):
            raise FileNotFoundError(f"Concatenation list file not created: {list_path}")
        
        # Verify the paths in the list file exist
        with open(list_path, 'r') as f:
            for line in f:
                file_path = line.strip()[6:-1]  # Extract path from "file 'path'"
                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"File in concat list does not exist: {file_path}")
        
        cmd = [
            FFMPEG_PATH,
            '-f', 'concat',
            '-safe', '0',
            '-i', list_path,
            '-c', 'copy',
            output_path
        ]
        logger.info("Concatenating upscaled videos...")
        subprocess.run(cmd, check=True)
        return output_path
    except subprocess.CalledProcessError as e:
        logger.error(f"FFmpeg concatenation error: {e}")
        # Debug: Print the contents of the concat list file
        if os.path.exists(list_path):
            with open(list_path, 'r') as f:
                logger.error(f"Contents of concat_list.txt:\n{f.read()}")
        return None
    except Exception as e:
        logger.error(f"Error during concatenation: {e}")
        return None
    finally:
        # Clean up intermediate files
        for file_path in intermediate_files:
            try:
                os.unlink(file_path)
            except OSError:
                pass
        try:
            os.unlink(list_path)
        except (OSError, NameError):
            pass
        try:
            os.rmdir(intermediate_dir)
        except OSError:
            pass

def upload_video(youtube, file_path, title, description, privacy="public"):
    """Upload video to YouTube"""
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return None

    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": ["compilation", "August 2025", "YouTube playlist"],
            "categoryId": "22"  # People & Blogs
        },
        "status": {
            "privacyStatus": privacy,
            "selfDeclaredMadeForKids": False
        }
    }

    try:
        media = MediaFileUpload(file_path, chunksize=-1, resumable=True)
        request = youtube.videos().insert(
            part=",".join(body.keys()),
            body=body,
            media_body=media
        )

        logger.info(f"Uploading {file_path}...")
        response = request.execute()
        video_id = response.get('id')

        logger.info(f"Upload successful! Video ID: {video_id}")
        return response
    except HttpError as e:
        logger.error(f"YouTube API error during upload: {e}")
    except Exception as e:
        logger.error(f"Upload error: {e}")
        return None

def main():
    # Create necessary directories
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Get authenticated YouTube service
    youtube = get_authenticated_service()
    if not youtube:
        logger.error("Failed to authenticate with YouTube")
        return
    
    # Get playlist ID from user
    playlist_id = "PLvwmjYrN9zHFA4Kr02GLkKxWFI1UnLFRY"
    if not playlist_id:
        logger.error("No playlist ID provided")
        return
    
    # Get videos from playlist
    logger.info(f"Fetching videos from playlist {playlist_id}...")
    videos = get_playlist_videos(youtube, playlist_id)
    if not videos:
        logger.error("No videos found in playlist or error fetching videos")
        return
    
    # Filter for August 2025 videos
    logger.info("Filtering for August 2025 videos...")
    august_videos = filter_august_2025_videos(videos)
    if not august_videos:
        logger.error("No videos found published in August 2025")
        return
    
    logger.info(f"Found {len(august_videos)} videos from August 2025")
    
    # Download videos
    downloaded_files = []
    for idx, video in enumerate(august_videos):
        filename = f"video_{idx}_{video['id']}.mp4"
        logger.info(f"Downloading {video['title']} ({video['id']})...")
        if download_video(video['id'], filename):
            downloaded_files.append(filename)
    
    if not downloaded_files:
        logger.error("No videos were successfully downloaded")
        return
    
    # Combine videos
    output_filename = "august_2025_compilation.mp4"
    logger.info("Combining videos...")
    combined_path = combine_videos(downloaded_files, output_filename)
    if not combined_path or not os.path.exists(combined_path):
        logger.error("Failed to combine videos")
        return
    
    # Upload combined video
    title = "August 2025 Video Compilation"
    description = "A compilation of all videos from my playlist published in August 2025.\n\n" \
                 "Automatically generated by a custom python script with ffmpeg."
    
    logger.info("Uploading combined video...")
    upload_response = upload_video(youtube, combined_path, title, description)
    
    if upload_response:
        # Create 4x version with LOFI audio
        spedup_path = create_spedup_version(combined_path)
        
        if spedup_path:
            # Upload the 4x version
            spedup_title = f"4x Speed - {title}"
            spedup_description = f"4x speed version of {title}\n\n{description}"
            
            logger.info("Uploading 4x speed version...")
            upload_video(youtube, spedup_path, spedup_title, spedup_description)
            
            # Rename original files to mark as uploaded
            os.rename(combined_path, combined_path.replace('.mp4', '_uploaded.mp4'))
            os.rename(spedup_path, spedup_path.replace('.mp4', '_uploaded.mp4'))
        
        # Cleanup all temporary files
        cleanup_files()
        logger.info("Process completed successfully!")
    else:
        logger.error("Failed to upload the original video")

if __name__ == "__main__":
    main()
