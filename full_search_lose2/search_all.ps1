$num_threads = 16
$parent_dir = "./full_search_output/"

if(Test-Path $parent_dir) {
  Remove-item $parent_dir
}
mkdir $parent_dir

Function RunOne($num_doves) {
  Write-Output ("Start:"+$num_doves)
  $filename = "{0:00}.tdl" -f $num_doves
  $target_path = $parent_dir + $filename
  cargo run --release $i $num_threads $target_path
  Write-Output ("Finished:"+$num_doves)
}

Function RunAll($min, $max) {
  for ($i=$min; $i -le $max; $i++) {
    Measure-Command { RunOne $i | Out-Default }
  }
}

Measure-Command { RunAll 2 12 | Out-Default }
Write-Output ("All Finished")

while ($true) {
  pause
}