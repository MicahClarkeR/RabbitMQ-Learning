dotnet run --project RceiveLogsTopic/ReceiveLogsTopic.csproj "kern.*"
dotnet run --project EmitLogsTopic/EmitLogsTopic.csproj kern.critical "A critical kernel error"