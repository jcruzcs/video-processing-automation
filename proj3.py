import argparse
import subprocess
import pymongo # type: ignore
from pymongo import MongoClient # type: ignore
from datetime import datetime
from datetime import timedelta
import pandas as panpan # type: ignore
import openpyxl # type: ignore
from PIL import Image # type: ignore
import io
import requests # type: ignore
import os
FRAME_IO_API_KEY = 'fio-u-9Q3xxrvR82MOL5sYB33ww66lJqH2M6lSuUM-wbnKWDuTcMqMrIR-Ynn4EXG53_xY'
FRAME_IO_PROJECT_ID = '080388ad-4b06-4802-b902-8e4d193fbb54'
FRAME_IO_FOLDER_ID = '9d81c470-6825-4b14-84fa-ea85d9f83d05'
#Jesse Cruz

def read_file_lines(filename):
    with open(filename, "r") as file:
        return file.readlines()

def parse_ranges(int_list):
    ranges = []
    start = int_list[0]
    end = int_list[0]
    for num in int_list[1:]:
        if num == end + 1:
            end = num
        else:
            if start == end:
                ranges.append(str(start))
            else:
                ranges.append(f"{start}-{end}")
            start = end = num
    if start == end:
        ranges.append(str(start))
    else:
        ranges.append(f"{start}-{end}")
    return ranges

def process_baselight_file(db):
    lines = read_file_lines('Baselight_export.txt')
    collection = db['baselight']
    count = 0
    for line in lines[:-1]:
        parts = line.split()
        numbers = [int(x) for x in parts[1:] if x not in ('<err>', '<null>', '\n', '<null>\n')]
        ranges = parse_ranges(numbers)
        for range_str in ranges:
            record = {"location": parts[0], "frames": range_str}
            collection.insert_one(record)
            count += 1
    print(f" {count} documents inserted in baselight collection") 
    

def process_xytech_file(db):
    filepath = 'Xytech.txt'  
    collection = db['xytech']
    details = {}
    locations = []
    current_key = None
    with open(filepath, 'r') as file:
        lines = file.read().splitlines()
    
    header = lines[0].strip()
    details['Workorder'] = header  

    for line in lines[1:]:
        line = line.strip()
        if line.endswith(':'):
            current_key = line[:-1]
            details[current_key] = []
        elif current_key and line:
            if current_key == "Location":
                locations.append(line)  # Collect locations separately
            else:
                details[current_key].append(line) 

    count = 0
    for location in locations:
        document = {key: value[0] if len(value) == 1 else value for key, value in details.items()}  
        document['Location'] = location  
        collection.insert_one(document)
        count += 1
    print(f" {count} documents inserted in xytech collection") 


def process_hpsans_file(db):
    rec = read_file_lines('Baselight_export.txt')
    tam = len(rec) - 1

    def ranger(xint_int):
        ranges = []
        start = xint_int[0]
        end = xint_int[0]
        for num in xint_int[1:]:
            if num == end + 1:
                end = num
            else:
                if start == end:
                    ranges.append(str(start))
                else:
                    ranges.append(f"{start}-{end}")
                start = end = num
        if start == end:
            ranges.append(str(start))
        else:
            ranges.append(f"{start}-{end}")
        return ranges

    records = []
    recordsfinal = []

    # Process each line of the text file
    for y in range(tam):
        x = rec[y].split(" ")
        filtro = [y for y in x[1:] if y != '<err>' and y != '<null>' and y != '\n' and y != '<null>\n']
        xint = [int(y) for y in filtro]
        rangos = ranger(xint)
        for i in rangos:
            records.append(x[0] + " " + i)

    for k in range(len(records) - 1):
        if "/baselightfilesystem1/Dune2/reel1/partA/1920x1080" in records[k]:
            recordsfinal.append(records[k].replace("/baselightfilesystem1/Dune2/reel1/partA/1920x1080", "/hpsans13/production/Dune2/reel1/partA/1920x1080"))
        if "/baselightfilesystem1/Dune2/reel1/VFX/Hydraulx" in records[k]:
            recordsfinal.append(records[k].replace("/baselightfilesystem1/Dune2/reel1/VFX/Hydraulx", "/hpsans12/production/Dune2/reel1/VFX/Hydraulx"))
        if "/baselightfilesystem1/Dune2/reel1/VFX/Framestore" in records[k]:
            recordsfinal.append(records[k].replace("/baselightfilesystem1/Dune2/reel1/VFX/Framestore", "/hpsans13/production/Dune2/reel1/VFX/Framestore"))
        if "/baselightfilesystem1/Dune2/reel1/VFX/AnimalLogic" in records[k]:
            recordsfinal.append(records[k].replace("/baselightfilesystem1/Dune2/reel1/VFX/AnimalLogic", "/hpsans14/production/Dune2/reel1/VFX/AnimalLogic"))
        if "/baselightfilesystem1/Dune2/reel1/partB/1920x1080" in records[k]:
            recordsfinal.append(records[k].replace("/baselightfilesystem1/Dune2/reel1/partB/1920x1080", "/hpsans13/production/Dune2/reel1/partB/1920x1080"))
        if "/baselightfilesystem1/Dune2/pickups/shot_1ab/1920x1080" in records[k]:
            recordsfinal.append(records[k].replace("/baselightfilesystem1/Dune2/pickups/shot_1ab/1920x1080", "/hpsans15/production/Dune2/pickups/shot_1ab/1920x1080"))
        if "/baselightfilesystem1/Dune2/pickups/shot_2b/" in records[k]:
            recordsfinal.append(records[k].replace("/baselightfilesystem1/Dune2/pickups/shot_2b/", "/hpsans11/production/Dune2/pickups/shot_2b/1920x1080"))
        if "/baselightfilesystem1/Dune2/reel1/partC/" in records[k]:
            recordsfinal.append(records[k].replace("/baselightfilesystem1/Dune2/reel1/partC/", "/hpsans17/production/Dune2/reel1/partC/1920x1080"))

    locations = []
    frames2 = []

    for line in recordsfinal:
        parts = line.split()
        if len(parts) == 2:
            location, frames = parts
            locations.append(location)
            frames2.append(frames)

    b = open('Xytech.txt', 'r')

    with open("Xytech.txt", "r") as text_file:
        loc = text_file.readlines()

    producer = None
    operator = None
    job = None
    notes = None
    location = None
    frames_to_fix = None

    # Process Xytech file
    notes_started = False
    for line in loc:
        if line.startswith("Producer:"):
            producer = line.split(": ")[1].strip() if len(line.split(": ")) > 1 else ""
        elif line.startswith("Operator:"):
            operator = line.split(": ")[1].strip() if len(line.split(": ")) > 1 else ""
        elif line.startswith("Job:"):
            job = line.split(": ")[1].strip() if len(line.split(": ")) > 1 else ""
        elif line.startswith("Notes:"):
            notes = ""
            notes_started = True
        elif line.startswith("Location:"):
            location = line.split(": ")[1].strip() if len(line.split(": ")) > 1 else ""
        elif line.startswith("Frames to Fix:"):
            frames_to_fix = line.split(": ")[1].strip() if len(line.split(": ")) > 1 else ""
        elif notes_started:
            notes += line.strip() + " "

    # Insert into MongoDB
    collection = db['hpsans']
    for loc, frames in zip(locations, frames2):
        document = {
            "Producer": producer,
            "Operator": operator,
            "Job": job,
            "Notes": notes,
            "Location": loc,
            "Frames to Fix": frames
        }
        collection.insert_one(document)
def format_timecode(seconds, fps=60):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    frames = int((seconds - int(seconds)) * fps)
    return f"{hours:02}:{minutes:02}:{secs:02}:{frames:02}"

def process_video_file(db, video_file):
    process_hpsans_file(db)
    print("collection baselight and xytech processed")


    collection_hpsans = db['hpsans']
    collection_render = db['render']
    
    print("Extracting timecodes from video...")

    result = subprocess.run(
        ['ffmpeg', '-i', video_file, '-vf', 'showinfo', '-f', 'null', '-'],
        capture_output=True, text=True
    )
    lines = result.stderr.split('\n')
    timecodes = []
    for line in lines:
        if 'pts_time:' in line:
            parts = line.split()
            for part in parts:
                if part.startswith('pts_time:'):
                    timecodes.append(float(part.split(':')[1]))

    video_timecodes = timecodes

    print("Processing documents from hpsans collection...")

    fps = 60

    for document in collection_hpsans.find():
        location = document.get('Location')
        frame_range = document.get('Frames to Fix')
        
        if '-' not in frame_range:
            print(f"Skipping invalid frame range: {frame_range}")
            continue

        try:
            start_frame, end_frame = map(int, frame_range.split('-'))
        except ValueError as e:
            print(f"Error parsing frame range {frame_range}: {e}")
            continue

        if start_frame < len(video_timecodes) and end_frame < len(video_timecodes):
            start_timecode = video_timecodes[start_frame]
            end_timecode = video_timecodes[end_frame]
            timecode_range = f"{format_timecode(start_timecode)}-{format_timecode(end_timecode)}"

            render_document = {
                "Location": location,
                "Frames": frame_range,
                "Timecode Range": timecode_range
            }
            collection_render.insert_one(render_document)
            print(f"Inserted document with location: {location}, frames: {frame_range}, timecode range: {timecode_range}")
def extract_thumbnail(video_file, timecode):
    
    result = subprocess.run(
        ['ffmpeg', '-i', video_file, '-ss', str(timecode), '-vframes', '1', '-f', 'image2pipe', '-'],
        capture_output=True
    )
    
    
    img = Image.open(io.BytesIO(result.stdout))
    img = img.resize((96, 74), Image.Resampling.LANCZOS)
    return img
    
def extract_clip(video_file, start_time, duration, output_file):
    print(f"Extracting clip from {start_time} for {duration} seconds into {output_file}")
    if duration <= 0:
        print(f"Invalid duration {duration}. Skipping extraction.")
        return False

    command = [
        'ffmpeg', '-i', video_file, '-ss', f"{start_time:.3f}", '-t', f"{duration:.3f}",
        '-c:v', 'libx264', '-c:a', 'aac', output_file
    ]
    result = subprocess.run(command, capture_output=True)
    if result.returncode != 0:
        print(f"Error extracting clip: {result.stderr.decode()}")
        return False
    else:
        print(f"Extracted clip: {output_file}")
        return True

def upload_to_frameio(filepath):
    url = f'https://api.frame.io/v2/assets/{FRAME_IO_FOLDER_ID}/children'
    headers = {
        "Authorization": f"Bearer {FRAME_IO_API_KEY}",
        "Content-Type": "application/json"
    }
    file_data = {
        "type": "file",
        "name": os.path.basename(filepath)
    }
    
    response = requests.post(url, json=file_data, headers=headers)
    
    
    
    data = response.json()
    
    

    upload_url = data['upload_urls'][0]
    print(f"Uploading clip")

    headers = {
        'x-amz-acl': 'private',
        'Content-Type': 'video/mp4'
    }
    
    with open(filepath, 'rb') as f:
        upload_response = requests.put(upload_url, data=f, headers=headers)
        
        if upload_response.status_code not in (200, 201):
            print(f"Error uploading file: {upload_response.status_code}")
            print(f"Response: {upload_response.text}")
            upload_response.raise_for_status()

    print(f"Uploaded ")

def sanitize_filename(filename):
    return filename.replace('/', '_').replace(':', '_').replace(' ', '_')

def output_excel_with_thumbnails(db, video_file):
    collection_render = db['render']
    documents = list(collection_render.find())
    data = []
    print("Capturing thumbnails and clips...")

    def convert_to_seconds(time_str):
        h, m, s, f = map(int, time_str.split(':'))
        return h * 3600 + m * 60 + s + f / 60.0

    for doc in documents:
        location = doc['Location']
        frames = doc['Frames']
        timecode_range = doc['Timecode Range']
        start_time, end_time = timecode_range.split('-')
        
        start_secs = convert_to_seconds(start_time)
        end_secs = convert_to_seconds(end_time)
        
        median_time = (start_secs + end_secs) / 2
        
        thumbnail = extract_thumbnail(video_file, median_time)
        data.append({
            "Location": location,
            "Frames": frames,
            "Timecode Range": timecode_range,
            "Thumbnail": thumbnail
        })
        print("Thumbnail created")

   

    
    df = panpan.DataFrame([{
        "Location": item["Location"],
        "Frames": item["Frames"],
        "Timecode Range": item["Timecode Range"],
        "Thumbnail": ""
    } for item in data])

    with panpan.ExcelWriter("render_output.xlsx", engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Render Data', index=False)
        workbook = writer.book
        worksheet = writer.sheets['Render Data']

        for idx, item in enumerate(data):
            thumbnail = item["Thumbnail"]
            if thumbnail:
                img_data = io.BytesIO()
                thumbnail.save(img_data, format='PNG')
                img_data.seek(0)
                img = openpyxl.drawing.image.Image(img_data)
                img.width, img.height = (96, 74)
                cell_location = f'D{idx + 2}'
                worksheet.add_image(img, cell_location)

    for doc in documents:
        location = doc['Location']
        frames = doc['Frames']
        timecode_range = doc['Timecode Range']
        start_time, end_time = timecode_range.split('-')

        start_secs = convert_to_seconds(start_time)
        end_secs = convert_to_seconds(end_time)
        duration = end_secs - start_secs

        # Replace invalid filename characters
        sanitized_location = sanitize_filename(location)
        sanitized_start_time = sanitize_filename(start_time)
        sanitized_end_time = sanitize_filename(end_time)
        output_file = f"clip_{sanitized_location}_{sanitized_start_time}_{sanitized_end_time}.mp4"
        if extract_clip(video_file, start_secs, duration, output_file):
            upload_to_frameio(output_file)
            os.remove(output_file)
            

def main():
    # Set up MongoDB connection
    client = MongoClient('localhost', 27017)
    db = client['proj3']  # Assume 'proj3' is the database name

    parser = argparse.ArgumentParser(description="Process files and store in MongoDB.")
    parser.add_argument("--baselight", action='store_true', help="Process Baselight export file")
    parser.add_argument("--xytech", action='store_true', help="Process Xytech data file")
    parser.add_argument("--hpsans", action='store_true', help="Process and insert hpsans data")
    parser.add_argument("--process", type=str)
    parser.add_argument("--output", type=str)

    args = parser.parse_args()

    if args.baselight:
        process_baselight_file(db)
    elif args.xytech:
        process_xytech_file(db)
    elif args.hpsans:
        process_hpsans_file(db)
    elif args.process:
        process_video_file(db, args.process)
    elif args.output:
        output_excel_with_thumbnails(db, args.output)
    client.close()

if __name__ == "__main__":
    main()
