import subprocess

# Llama AppleScript que intenta controlar Excel
applescript = '''
tell application "Microsoft Excel"
    activate
end tell
'''

subprocess.run(["osascript", "-e", applescript])
