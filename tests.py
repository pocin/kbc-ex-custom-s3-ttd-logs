import subprocess
from main import combine_chunks
import pytest

@pytest.mark.parametrize('clean_up', [True, False])
def test_combining_chunks(tmpdir, clean_up):


    temp_logs = tmpdir.mkdir("temporary")
    final_logs = tmpdir.mkdir("combined").mkdir("slice.csv")

    for i in 'ab':
        pth = temp_logs.join(i + ".log")
        pth.write('''{i}\\\t{i}
{i}{i}\t{i}{i}\n'''.format(i=i))
        subprocess.check_call(['gzip', pth.strpath])
    assert len(temp_logs.listdir()) == 2
    outpath = combine_chunks(temp_logs.strpath, final_logs.strpath, clean_tmp_dir=clean_up)

    if clean_up:
        assert len(temp_logs.listdir()) == 0
    else:
        assert len(temp_logs.listdir()) == 2

    with open(outpath) as f:
        real = f.read()

    # expected = '\n'.join(list('abcd')) +'\n'
    expected = '''a\ta
aa\taa
b\tb
bb\tbb
'''

    assert expected == real
