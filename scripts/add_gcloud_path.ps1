$gcloudPath = "C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\bin"
$currentPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($currentPath -notlike "*$gcloudPath*") {
    $newPath = $currentPath + ";" + $gcloudPath
    [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
    Write-Host "gcloud added to PATH"
} else {
    Write-Host "gcloud already in PATH"
}
