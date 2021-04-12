param($first, $second)
Get-Content "$first$second" -Encoding Default | Out-File "data\$second" -Encoding utf8	