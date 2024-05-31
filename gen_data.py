from tqdm import tqdm

SIMULATED_DATA_FILE="examples/simulated.csv"
NUM_NMIS=1500000
NUM_NMI_SUFFIXES=2
NUM_DAYS=30

with open(SIMULATED_DATA_FILE, "w+") as outfile:

    with tqdm(total=NUM_NMIS*NUM_NMI_SUFFIXES*NUM_DAYS) as pbar:
        for nmi_count in range(NUM_NMIS):
            for suffix in range(NUM_NMI_SUFFIXES):
                outfile.write(f"200,{nmi_count},E1Q1B1K1,B1,B1,N1,02022,KWH,30,\n")
                for day in range(NUM_DAYS):
                    d = f"{day+1}" if day >= 9 else f"0{day+1}"
                    outfile.write(f"300,20050404,0.000,528.424,548.986,555.168,570.014,574.525,590.147,597.527,592.620,603.292,589.413,596.274,588.939,566.130,535.994,492.798,396.286,409.435,391.912,339.706,371.922,490.791,364.175,282.039,231.749,218.746,184.722,192.926,142.002,147.422,158.198,130.290,155.982,177.037,225.564,276.544,299.476,264.037,299.541,346.853,553.677,534.051,467.345,476.696,532.021,542.119,561.776,592.272,A,,,20050405003650,\n")
                    pbar.update(1)