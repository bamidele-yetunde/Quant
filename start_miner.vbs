' start_miner.vbs - starts the miner hidden in the background (local mode only).
' Needs BRAIN_EMAIL, BRAIN_PASSWORD and MINER_STATE_KEY set as user environment variables
' (e.g. setx BRAIN_EMAIL "you@example.com"), or a .miner_key file for the key.
' Disable the GitHub workflow first: never mine locally and in the cloud at the same time.
' Output goes to miner\console.log; stop it from Task Manager (python.exe) or with Ctrl+C in a terminal run.
Option Explicit
Dim sh, fso, dir
Set sh = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
dir = fso.GetParentFolderName(WScript.ScriptFullName)
sh.CurrentDirectory = dir
If Not fso.FolderExists(dir & "\miner") Then fso.CreateFolder(dir & "\miner")
sh.Run "cmd /c python miner.py >> miner\console.log 2>&1", 0, False
