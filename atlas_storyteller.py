import os
import openai
import json
import re
import concurrent.futures
import time
import csv

MAX_RETRIES = 5

def generate_text_files():
    # Read the API key from 'apikey.txt'
    with open('apikey.txt', 'r', encoding='utf-8') as f:
        openai.api_key = f.read().strip()

    # Open the JSONL file and load the topics and subtopics
    with open('synthetic-short-stories.jsonl', 'r') as f:
        lines = f.readlines()

    tasks = []
    task_set = set()  # Define the task_set as an empty set
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:  ##   WORKERS 
        futures = []
        for line in lines:
            topic_data = json.loads(line)
            topic = topic_data["topic"]
            subtopics = topic_data["subtopics"]

            for subtopic in subtopics:
                task = {
                    "topic": topic,
                    "subtopic": subtopic,
                    "retries": MAX_RETRIES
                }
                tasks.append(task)

        for task in tasks:
            task_set.add(str(task))  # Add every task to the task_set
            futures.append(executor.submit(make_request, task))

        while futures:
            done, futures = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)

            for future in done:
                task = future.result()
                if "exception" in task:
                    if task["retries"] > 0:
                        task["retries"] -= 1
                        print(f"Error on topic '{task['topic']}' and subtopic '{task['subtopic']}', retrying. {task['retries']} retries left.")
                        futures.add(executor.submit(make_request, task))
                    else:
                        print(f"Error on topic '{task['topic']}' and subtopic '{task['subtopic']}', no retries left.")
                else:
                    task_set.remove(str(task))  # If the task was successful, remove it from the task_set
                    print(f"Completed topic '{task['topic']}' and subtopic '{task['subtopic']}'.")

    # Check if the set is empty at the end of processing
    if not task_set:
        print("All tasks have been completed successfully.")
    else:
        print(f"Following tasks were not completed: {task_set}")

    # Check the existence of the output files in the directory for each task.
    for task in tasks:
        topic = task["topic"]
        subtopic = task["subtopic"]
        if not os.path.isfile(f'output/{topic}_{subtopic}.txt'):
            print(f"Output file for task ({topic}, {subtopic}) does not exist.")
        else:
            print(f"Output file for task ({topic}, {subtopic}) is successfully written.")

def make_request(task):
    topic = task["topic"]
    subtopic = task["subtopic"]
    retries = task["retries"]

    # Check if the file exists, if yes, return the task and skip the rest
    output_file = f'output/{topic}_{subtopic}.txt'
    if os.path.isfile(output_file):
        print(f"File '{output_file}' already exists, skipping request.")
        return task

    print(f"\nInitiating request for topic '{topic}' and subtopic '{subtopic}' with {retries} retries left.")

    try:
        messages=[
            {
                "role": "system",
                "content": "You are an AI storyteller. Your task is to weave a tale about a certain topic and subtopic. There are three parts to this task: creating a beginning (this is where you introduce the characters and the setting), creating a middle (this is where the main events and problems take place), and creating an end (this is where the problems are solved and the story concludes). The story should feel real, but be made up. Be direct and clear. Use detailed language to paint a picture with your words."
            },
            {
                "role": "user",
                "content": f"Please weave a narrative tapestry around the central theme of '{topic}' and the intertwined sub-theme of '{subtopic}'. The story should be clearly defined into three distinct sections: a narrative prompt, a plot development, and a resolution. Endeavor to imbue the language with rich descriptions, yet ensure it remains clear, comprehensible, and devoid of superfluous language. Your story should possess the sophistication and intricacy that would be commensurate with a doctoral level literature study. Please craft the narrative in such a manner that it reaches a word count of 1,500. The narrative should flow seamlessly from one part to another, rather than being fragmented into separate sections deliniated by 'part 1' or 'section 1'. Do not put 'Part 1:' anywhere in the response. Make the entire story a minimum of 15 paragraphs, each paragraph containing at least 5 sentences. Use stylings from American literature and follow the normal tropes that are prevelent in {topic} and {subtopic}."
            }
        ]

        completion = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=messages,
            max_tokens=3000  # Adjust this as needed
        )
        # Prepare the response
        response = f"{completion.choices[0].message['content']}"

        # Output the response to a text file named after the topic and subtopic
        with open(f'output/{topic}_{subtopic}.txt', 'w', encoding='utf-8') as f:
            f.write(response)

        print(f"Successful request for topic '{topic}' and subtopic '{subtopic}'. Writing to file.")
    except Exception as e:
        print(f"Exception occurred for topic '{topic}' and subtopic '{subtopic}'.")
        task["exception"] = e
    finally:
        return task

task_id_counter = 0  # Global counter for task IDs

def cleanse_text(line):
    global task_id_counter
    clean_line = line.replace('\n', ' ')  # Remove newlines
    clean_line = re.sub(r'^\d+\.\s*', '', clean_line)  # Removing leading number and period
    match_name = re.search(r"'name': '([^']+)'", clean_line)  # Using regex to find and replace 'name' with modified 'name'

    if match_name:
        old_name = match_name.group(1)
        new_name = old_name.lower().replace(' ', '_')
        clean_line = clean_line.replace(old_name, new_name)

    match_id = re.search(r"'id': '([^']+)'", clean_line)  # Replace 'id' with a new one
    if match_id:
        old_id = match_id.group(1)
        new_id = f'seed_task_{task_id_counter}'
        clean_line = clean_line.replace(old_id, new_id)
        task_id_counter += 1

    return clean_line


def text_to_csv(dir_path, output_file):
    global task_id_counter
    files_list = os.listdir(dir_path)
    text_files_list = [file for file in files_list if file.endswith('.txt')]

    with open(output_file, 'w', encoding='utf-8', newline='') as csv_file:
        writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)  # Create a csv.writer
        writer.writerow(['id', 'Story'])  # Write header

        for text_file in text_files_list:
            with open(os.path.join(dir_path, text_file), 'r', encoding='utf-8') as f:
                full_story = f.read()  # Read the whole file content
                full_story = cleanse_text(full_story).strip()

                # Write to CSV directly, avoiding triple quotes
                writer.writerow([f'seed_task_{task_id_counter}', full_story])
                task_id_counter += 1

def main():
    if not os.path.exists('output'):
        os.makedirs('output')
    generate_text_files()
    dir_path = 'output/'
    output_file = 'output.csv'
    text_to_csv(dir_path, output_file)


if __name__ == "__main__":
    main()
