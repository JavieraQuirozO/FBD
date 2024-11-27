from FlyBaseDownloads.downloads.Downloads import Downloads 
from FlyBaseDownloads.utilities.internet import Check_internet

class AnnSeq():
    

    def get(self, data_type = "chromosome"):
        FASTA_URL = "ftp://ftp.flybase.net/releases/current/dmel_r6.60/fasta/"
        url = str(f"{FASTA_URL}dmel-all-{data_type}-r6.60.fasta.gz")
        
        internet = Check_internet.check_internet_connection(msg=False)
        downloads = Downloads(url, internet)
        
        file = downloads.get()
        
        return file
    
    def get_types(self):
        list_types = ["aligned", "CDS", "chromosome",
                      "clones", "exon", "five_prime_UTR",
                      "gene", "gene_extended2000", "intergenic", 
                      "intron", "miRNA", "miscRNA", 
                      "ncRNA", "predicted", "pseudogene",
                      "sequence_features", "synteny", 
                      "three_prime_UTR", "transcript", 
                      "translation", "transposon", "tRNA"]
        
        return list_types
