Group 9 project for CS511 - Spring 2024


## wsl cleanup
fstrim -a --------in wsl



wsl --shutdown
diskpart
# open window Diskpart
select vdisk file=".\ext4.vhdx"
attach vdisk readonly
compact vdisk
detach vdisk
exit