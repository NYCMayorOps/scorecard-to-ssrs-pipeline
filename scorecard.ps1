$out = python bulk_backfill.py;
if ($LASTEXITCODE -ne 0) {Write-Host $out; exit 1}