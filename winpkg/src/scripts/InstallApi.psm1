### Licensed to the Apache Software Foundation (ASF) under one or more
### contributor license agreements.  See the NOTICE file distributed with
### this work for additional information regarding copyright ownership.
### The ASF licenses this file to You under the Apache License, Version 2.0
### (the "License"); you may not use this file except in compliance with
### the License.  You may obtain a copy of the License at
###
###     http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.

###
### A set of basic PowerShell routines that can be used to install and
### manage Hadoop services on a single node. For use-case see install.ps1.
###

###
### Global variables
###
$ScriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)
$FinalName = "@final.name@"


###############################################################################
###
### Installs phoenix component.
###
### Arguments:
###     component: Component to be installed, it should be phoenix
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     role: phoenix
###
###############################################################################
function Install(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$false )]
    $serviceCredential,
    [String]
    [Parameter( Position=3, Mandatory=$false )]
    $role
    )
{
    if ( $component -eq "phoenix" )
    {
        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"

	    ### $phoenixInstallPath: the name of the folder containing the application, after unzipping
	    $phoenixInstallPath = Join-Path $nodeInstallRoot $FinalName

	    Write-Log "Installing Apache phoenix @final.name@ to $phoenixInstallPath"

        ### Create Node Install Root directory
        if( -not (Test-Path "$nodeInstallRoot"))
        {
            Write-Log "Creating Node Install Root directory: `"$nodeInstallRoot`""
            $cmd = "mkdir `"$nodeInstallRoot`""
            Invoke-CmdChk $cmd
        }


        ###
        ###  Unzip Hadoop distribution from compressed archive
        ###
        Write-Log "Extracting $FinalName.zip to $phoenixInstallPath"
        if ( Test-Path ENV:UNZIP_CMD )
        {
            ### Use external unzip command if given
            $unzipExpr = $ENV:UNZIP_CMD.Replace("@SRC", "`"$HDP_RESOURCES_DIR\$FinalName.zip`"")
            $unzipExpr = $unzipExpr.Replace("@DEST", "`"$nodeInstallRoot`"")
            ### We ignore the error code of the unzip command for now to be
            ### consistent with prior behavior.
            Invoke-Ps $unzipExpr
        }
        else
        {
            $shellApplication = new-object -com shell.application
            $zipPackage = $shellApplication.NameSpace("$HDP_RESOURCES_DIR\$FinalName.zip")
            $destinationFolder = $shellApplication.NameSpace($nodeInstallRoot)
            $destinationFolder.CopyHere($zipPackage.Items(), 20)
        }

        ###
        ###  Copy template config files
        ###
        #$xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template\conf\*.xml`" `"$phoenixInstallPath\conf`""
        #Invoke-Cmd $xcopy_cmd

        ###
        ### Set PHOENIX_HOME environment variable
        ###
        Write-Log "Setting the PHOENIX_HOME environment variable at machine scope to `"$phoenixInstallPath`""
        [Environment]::SetEnvironmentVariable("PHOENIX_HOME", $phoenixInstallPath, [EnvironmentVariableTarget]::Machine)
        $ENV:PHOENIX_HOME = "$phoenixInstallPath"
		###
        ### Copying PHOENIX jars to HBASE lib
        ###
        Write-Log "Copying PHOENIX jars to HBASE lib"
        $xcopy_cmd = "xcopy /EIYF `"$ENV:PHOENIX_HOME\phoenix-*-server.jar`" `"$ENV:HBASE_HOME\lib`""
        Invoke-Cmd $xcopy_cmd

        ###
        ### Creating Phoenix Query Server Service
        ###
        $service= "queryserver"
        $phoenixInstallToBin = join-path $phoenixInstallPath "bin"
        CreateAndConfigureHadoopService $service $HDP_RESOURCES_DIR $phoenixInstallToBin $serviceCredential

        ###
        ### Setup Phoenix Query Server service config
        ###
        Write-Log "Creating configuration for PQS service"
        Write-Log "Creating service config ${phoenixInstallToBin}\$service.xml"
        $cmd = "python.exe $phoenixInstallToBin\queryserver.py makeWinServiceDesc > `"$phoenixInstallToBin\$service.xml`""
        Invoke-CmdChk $cmd

        Write-log "$env:HADOOP_NODE_INSTALL_ROOT"

        Write-Log "Finished installing Apache phoenix"
    }
    else
    {
        throw "Install: Unsupported compoment argument."
    }
}

###############################################################################
###
### Uninstalls phoenix component.
###
### Arguments:
###     component: Component to be uninstalled.
###     nodeInstallRoot: Install folder (for example "C:\Hadoop")
###
###############################################################################
function Uninstall(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot
    )
{
    if ( $component -eq "phoenix" )
    {
        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"

        Write-Log "Uninstalling Apache phoenix $FinalName"
        $phoenixInstallPath = Join-Path $nodeInstallRoot $FinalName
        StopAndDeleteHadoopService "queryserver"

        ### If Hadoop Core root does not exist exit early
        if ( -not (Test-Path $phoenixInstallPath) )
        {
            return
        }

        ###
        ### Delete install dir
        ###
        $cmd = "rd /s /q `"$phoenixInstallPath`""
        Invoke-Cmd $cmd

        ### Removing PHOENIX_HOME environment variable
        Write-Log "Removing the PHOENIX_HOME environment variable"
        [Environment]::SetEnvironmentVariable( "PHOENIX_HOME", $null, [EnvironmentVariableTarget]::Machine )

        Write-Log "Successfully uninstalled phoenix"

    }
    else
    {
        throw "Uninstall: Unsupported compoment argument."
    }
}

###############################################################################
###
### Alters the configuration of the phoenix component.
###
### Arguments:
###     component: Component to be configured, it should be "phoenix"
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     configs:
###
###############################################################################
function Configure(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$false )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=3 )]
    $configs = @{},
    [bool]
    [parameter( Position=4 )]
    $aclAllFolders = $True
    )
{

    if ( $component -eq "phoenix" )
    {
        Write-Log "Configure: phoenix does not have any configurations"
    }
    else
    {
        throw "Configure: Unsupported compoment argument."
    }
}

###############################################################################
###
### Start component services.
###
### Arguments:
###     component: Component name
###     roles: List of space separated service to start
###
###############################################################################
function StartService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $roles
    )
{
    Write-Log "Starting `"$component`" `"$roles`" services"

    if ( $component -eq "phoenix" )
    {
        Write-Log "StartService: phoenix does not have any services"
    }
    else
    {
        throw "StartService: Unsupported compoment argument."
    }
}

###############################################################################
###
### Stop component services.
###
### Arguments:
###     component: Component name
###     roles: List of space separated service to stop
###
###############################################################################
function StopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $roles
    )
{
    Write-Log "Stopping `"$component`" `"$roles`" services"

    if ( $component -eq "phoenix" )
    {
        Write-Log "StopService: phoenix does not have any services"
    }
    else
    {
        throw "StartService: Unsupported compoment argument."
    }
}

### Creates and configures the service.
function CreateAndConfigureHadoopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $service,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $hdpResourcesDir,
    [String]
    [Parameter( Position=2, Mandatory=$true )]
    $serviceBinDir,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=3, Mandatory=$true )]
    $serviceCredential
)
{
    if ( -not ( Get-Service "$service" -ErrorAction SilentlyContinue ) )
    {
        Write-Log "Creating service `"$service`" as $serviceBinDir\$service.exe"
        $xcopyServiceHost_cmd = "copy /Y `"$hdpResourcesDir\serviceHost.exe`" `"$serviceBinDir\$service.exe`""
        Invoke-CmdChk $xcopyServiceHost_cmd

        #HadoopServiceHost.exe will write to this log but does not create it
        #Creating the event log needs to be done from an elevated process, so we do it here
        if( -not ([Diagnostics.EventLog]::SourceExists( "$service" )))
        {
            [Diagnostics.EventLog]::CreateEventSource( "$service", "" )
        }

        Write-Log "Adding service $service"
        $s = New-Service -Name "$service" -BinaryPathName "$serviceBinDir\$service.exe" -Credential $serviceCredential -DisplayName "Apache Hadoop $service"
        if ( $s -eq $null )
        {
            throw "CreateAndConfigureHadoopService: Service `"$service`" creation failed"
        }

        $cmd="$ENV:WINDIR\system32\sc.exe failure $service reset= 30 actions= restart/5000"
        Invoke-CmdChk $cmd

        $cmd="$ENV:WINDIR\system32\sc.exe config $service start= demand"
        Invoke-CmdChk $cmd

        Set-ServiceAcl $service
    }
    else
    {
        Write-Log "Service `"$service`" already exists, Removing `"$service`""
        StopAndDeleteHadoopService $service
        CreateAndConfigureHadoopService $service $hdpResourcesDir $serviceBinDir $serviceCredential
    }
}

### Stops and deletes the Hadoop service.
function StopAndDeleteHadoopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $service
)
{
    Write-Log "Stopping $service"
    $s = Get-Service $service -ErrorAction SilentlyContinue

    if( $s -ne $null )
    {
        Stop-Service $service
        $cmd = "sc.exe delete $service"
        Invoke-Cmd $cmd
    }
}

###
### Public API
###
Export-ModuleMember -Function Install
Export-ModuleMember -Function Uninstall
Export-ModuleMember -Function Configure
Export-ModuleMember -Function StartService
Export-ModuleMember -Function StopService
