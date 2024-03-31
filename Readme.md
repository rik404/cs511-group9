Group 9 project for CS511 - Spring 2024


## wsl cleanup
sudo docker system prune
fstrim -a --------in wsl



wsl --shutdown
diskpart
# open window Diskpart
select vdisk file="C:\Users\mohan\AppData\Local\packages\CanonicalGroupLimited.Ubuntu_79rhkp1fndgsc\LocalState\ext4.vhdx"
attach vdisk readonly
compact vdisk
detach vdisk
exit