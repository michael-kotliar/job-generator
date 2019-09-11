import sys
import os

with open(sys.argv[1], 'r') as s:
    for l in s:
        if "run:" in l:
            indentation = l.split("run:")[0]
            p = os.path.join(os.getcwd(),l.split(":")[1].strip())
            print(indentation+"run:")
            with open(p, 'r') as ss:
                for ll in ss:
                    if "s:mainEntity" in ll or "$import" in ll:
                        continue
                    else:
                        print(indentation+indentation+ll.rstrip())
        else:
            print(l.rstrip())