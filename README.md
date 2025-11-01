# Downloader (Tauri + React + TypeScript)

This project ships a desktop downloader built with Tauri 2, React 19, and Vite. The guide below explains how to prepare your environment and produce installers for Linux and Windows.

## Prerequisites

Install these tools once on each build machine:

- [Node.js](https://nodejs.org/) 18+ with [pnpm](https://pnpm.io/) (`corepack enable` or `npm i -g pnpm`)
- [Rust toolchain](https://www.rust-lang.org/tools/install) (stable channel, includes `cargo`)
- Tauri CLI is already listed in `devDependencies`; run it through `pnpm tauri ...`

### Additional platform packages

| Platform | Requirements |
| --- | --- |
| **Linux** | `sudo apt install libwebkit2gtk-4.1-0 libwebkit2gtk-4.1-dev libxdo-dev libssl-dev libayatana-appindicator3-dev librsvg2-dev` (Debian/Ubuntu). Replace with equivalents on other distros. |
| **Windows 10/11** | Install the **Desktop development with C++** workload from Visual Studio Build Tools 2019/2022, plus the [WebView2 runtime](https://developer.microsoft.com/microsoft-edge/webview2/) (Windows 11 includes it). |

> Building Windows bundles on Linux/macOS is not officially supported by Tauri. Use a native Windows machine or CI runner for `.msi` output.

## Install dependencies

```bash
pnpm install
```

## Development preview

```bash
pnpm tauri dev
```

## Production builds

`pnpm tauri build` compiles the Rust backend, builds the React frontend, and creates installers under `src-tauri/target/release/bundle/`.

### Linux

```bash
# Build on a Linux host with the packages listed above
pnpm tauri build

# Optional: target a specific architecture (e.g. x86_64)
pnpm tauri build --target x86_64-unknown-linux-gnu
```

Artifacts may include `.AppImage`, `.deb`, and tar archives.

### Windows 10/11

Run the build on a Windows host after installing the Build Tools.

```powershell
pnpm tauri build

# Optional: explicitly state the MSVC target
pnpm tauri build --target x86_64-pc-windows-msvc
```

Outputs include an `.msi` installer and a zipped executable.

### Notes

- The default `src-tauri/tauri.conf.json` sets `bundle.targets` to `"all"`, so the appropriate format for the host OS is generated automatically.
- Update icons in `src-tauri/icons/` before packaging if you need custom branding.
- Use `pnpm tauri build --debug` or `cargo clean -p downloader_lib` if you run into caching issues.

## Recommended IDE setup

- [VS Code](https://code.visualstudio.com/)
- [Tauri VS Code extension](https://marketplace.visualstudio.com/items?itemName=tauri-apps.tauri-vscode)
- [rust-analyzer](https://marketplace.visualstudio.com/items?itemName=rust-lang.rust-analyzer)
