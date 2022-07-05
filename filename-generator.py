from subprocess import Popen, PIPE

process = Popen(['aws', 's3', 'cp', 's3://sagemaker-studio-qt0kal0xm2/vox1_test_wav.zip', '-'], stdout=PIPE, stderr=PIPE)
# process = Popen(['cat', 'filelist.txt'], stdout=PIPE, stderr=PIPE)
stdout, stderr = process.communicate()
stdout, stderr = stdout.decode('utf-8'), stderr.decode('utf-8')

with open("filelist-2.txt", 'w') as f:
    for line in stdout.split('\n')[3:-3]:
        f.write(line.split('  05-29')[0].strip() + ' wav/' + line.split('   wav/')[1].strip() + '\n')
