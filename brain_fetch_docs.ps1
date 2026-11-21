# brain_fetch_docs.ps1 - downloads BRAIN's documentation and operator list into docs/ (git-ignored).
# Usage:  powershell -ExecutionPolicy Bypass -File .\brain_fetch_docs.ps1
# Uses $env:BRAIN_EMAIL / $env:BRAIN_PASSWORD, or asks for them.
param(
    [string]$Email = $env:BRAIN_EMAIL,
    [string]$Password = $env:BRAIN_PASSWORD
)
$ErrorActionPreference = 'Stop'
$api = 'https://api.worldquantbrain.com'
$out = Join-Path $PSScriptRoot 'docs'
New-Item -ItemType Directory -Force $out | Out-Null

if (-not $Email) { $Email = Read-Host 'BRAIN email' }
if (-not $Password) {
    $sec = Read-Host 'BRAIN password' -AsSecureString
    $Password = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
        [Runtime.InteropServices.Marshal]::SecureStringToBSTR($sec))
}

$basic = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("${Email}:${Password}"))
$session = New-Object Microsoft.PowerShell.Commands.WebRequestSession
try {
    Invoke-WebRequest -Uri "$api/authentication" -Method Post -Headers @{ Authorization = "Basic $basic" } `
        -WebSession $session -UseBasicParsing | Out-Null
} catch {
    Write-Error "Login failed. If BRAIN asks for an identity check, log in once in a browser and retry. $_"
}

function Get-Brain([string]$path) {
    Invoke-RestMethod -Uri "$api$path" -WebSession $session -UseBasicParsing
}

function ConvertTo-PlainText([string]$html) {
    if (-not $html) { return '' }
    $t = $html -replace '<br\s*/?>', "`n" -replace '</(p|div|li|h\d)>', "`n" -replace '<li>', '- '
    $t = $t -replace '<[^>]+>', ''
    return [System.Net.WebUtility]::HtmlDecode($t).Trim()
}

$md = New-Object System.Text.StringBuilder
[void]$md.AppendLine('# WorldQuant BRAIN documentation (downloaded via the API)')
[void]$md.AppendLine("Downloaded $(Get-Date -Format 'yyyy-MM-dd HH:mm'). WorldQuant's content: do not commit.")
[void]$md.AppendLine()

# Operators
Write-Host 'Fetching operators...'
$ops = Get-Brain '/operators'
$ops | ConvertTo-Json -Depth 10 | Out-File -Encoding utf8 (Join-Path $out 'operators.json')
[void]$md.AppendLine('## Operators')
[void]$md.AppendLine()
[void]$md.AppendLine('| Name | Category | Definition | Description |')
[void]$md.AppendLine('|---|---|---|---|')
foreach ($o in $ops) {
    $desc = ($o.description -replace '\|', '/' -replace "`r?`n", ' ')
    $def = ($o.definition -replace '\|', '/')
    [void]$md.AppendLine("| $($o.name) | $($o.category) | ``$def`` | $desc |")
}
[void]$md.AppendLine()

# Tutorials / documentation pages
Write-Host 'Fetching documentation pages...'
try {
    $tut = Get-Brain '/tutorials'
    $tut | ConvertTo-Json -Depth 10 | Out-File -Encoding utf8 (Join-Path $out 'tutorials.json')
    $sections = if ($tut.results) { $tut.results } else { $tut }
    foreach ($sec in $sections) {
        [void]$md.AppendLine("## $($sec.title)")
        [void]$md.AppendLine()
        foreach ($pg in $sec.pages) {
            try {
                $page = Get-Brain "/tutorial-pages/$($pg.id)"
                [void]$md.AppendLine("### $($page.title)")
                [void]$md.AppendLine()
                foreach ($block in $page.content) {
                    if ($block.type -eq 'TEXT' -or $block.value -is [string]) {
                        [void]$md.AppendLine((ConvertTo-PlainText $block.value))
                        [void]$md.AppendLine()
                    }
                }
                Start-Sleep -Milliseconds 300
            } catch {
                Write-Warning "page $($pg.id): $_"
            }
        }
    }
} catch {
    Write-Warning "Documentation pages unavailable: $_"
}

$md.ToString() | Out-File -Encoding utf8 (Join-Path $out 'ALL_DOCS.md')
Write-Host "Saved docs\ALL_DOCS.md and docs\operators.json"
