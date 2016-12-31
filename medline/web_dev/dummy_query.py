# coding=utf-8

from random import random
import json


def dummy_query(query_str):
    '''
    Placeholder to get web development started.
    Returns a json response with sample data
    :param query_str:
    :return:
    '''

    sample_articles_1 = [ 'Nacka-Aleksić M, Pilipović I, Stojić-Vukanić Z, Kosec D, Bufan B, Vujnović I, Arsenović-Ranin N, Dimitrijević M, Leposavić G. Sexual dimorphism in the aged rat CD4+ T lymphocyte-mediated immune response elicited by inoculation with spinal cord homogenate. Mech Ageing Dev. 2015 Sep 22. pii: S0047-6374(15)30015-4.',
                                      'Matos DM, Furtado FM, Falcão RP. Monoclonal B-cell lymphocytosis in, individuals from sporadic (non-familial) chronic lymphocytic leukemia families, persists over time, but does not progress to chronic B-cell lymphoproliferative, diseases. Rev Bras Hematol Hemoter. 2015 Sep-Oct;37(5):292-5.',
                                      'Gambhir L, Checker R, Sharma D, Thoh M, Patil A, Degani M, Gota V, Sandur SK. Thiol dependent NF-κB suppression and inhibition of T-cell mediated adaptive immune responses by a naturally occuring steroidal lactone withaferin a. Toxicol Appl Pharmacol. 2015 Sep 22. pii: S0041-008X(15)30088-0.',
                                      'Kumar J, Kale V, Limaye L. Umbilical cord blood-derived CD11c(+) dendritic cells could serve as an alternative allogeneic source of dendritic cells for cancer immunotherapy. Stem Cell Res Ther. 2015 Sep 25;6(1):184.',
                                      'McCarthy MK, Procario MC, Wilke CA, Moore BB, Weinberg JB. Prostaglandin E2 Production and T Cell Function in Mouse Adenovirus Type 1 Infection following Allogeneic Bone Marrow Transplantation. PLoS One. 2015 Sep 25;10(9):e0139235.',
                                      'Witte K, Koch E, Volk HD, Wolk K, Sabat R. The Pelargonium sidoides Extract EPs 7630 Drives the Innate Immune Defense by Activating Selected MAP Kinase Pathways in Human Monocytes. PLoS One. 2015 Sep 25;10(9):e0138075.',
                                      'Trapani JA, Voskoboinik I, Jenkins MR. Perforin-dependent cytotoxicity: "Kiss of death" or prolonged embrace with darker elocation-idnseque11es? Oncoimmunology. 2015 Apr 14;4(9):e1036215.',
                                      'Ames E, Canter RJ, Grossenbacher SK, Mac S, Smith RC, Monjazeb AM, Chen M, Murphy WJ. Enhanced targeting of stem-like solid tumor cells with radiation and natural killer cells. Oncoimmunology. 2015 Jun 5;4(9):e1036212.',
                                      'Brisson L, Carrier A. A novel actor in antitumoral immunity: The thymus-specific serine protease TSSP/PRSS16 involved in CD4(+) T-cell maturation. Oncoimmunology. 2015 Apr 2;4(9):e1026536. eCollection 2015 Sep.',
                                      'Mina M, Boldrini R, Citti A, Romania P, D\'Alicandro V, De Ioris M, Castellano A, Furlanello C, Locatelli F, Fruci D. Tumor-infiltrating T lymphocytes improve clinical outcome of therapy-resistant neuroblastoma. Oncoimmunology. 2015 Apr 2;4(9):e1019981.'
                        ]

    sample_articles_2 = ['Parida SK, Poiret T, Zhenjiang L, Meng Q, Heyckendorf J, Lange C, Ambati AS, Rao MV, Valentini D, Ferrara G, Rangelova E, Dodoo E, Zumla A, Maeurer M. T-Cell Therapy: Options for Infectious Diseases. Clin Infect Dis. 2015 Oct 15;61(suppl 3):S217-S224.',
                                     'Trampush JW, Lencz T, DeRosse P, John M, Gallego JA, Petrides G, Hassoun Y, Zhang JP, Addington J, Kellner CH, Tohen M, Burdick KE, Goldberg TE, Kane JM, Robinson DG, Malhotra AK. Relationship of Cognition to Clinical Response in First-Episode Schizophrenia Spectrum Disorders. Schizophr Bull. 2015 Sep 25. pii: sbv120.',
                                     'Alkabab YM, Al-Abdely HM, Heysell SK. Diabetes related tuberculosis in the Middle East: an urgent need for regional research. Int J Infect Dis. 2015 Sep 23. pii: S1201-9712(15)00220-9. doi: 10.1016/j.ijid.2015.09.010. [Epub ahead of print] Review.',
                                     'ICECaP Working Group. The Development of Intermediate Clinical Endpoints in Cancer of the Prostate (ICECaP). J Natl Cancer Inst. 2015 Sep 25;107(12). pii: djv261.',
                                     'De Souza R, Spence T, Huang H, Allen C. Preclinical imaging and translational  animal models of cancer for accelerated clinical implementation of nanotechnologies and macromolecular agents. J Control Release. 2015 Sep 23. pii:  S0168-3659(15)30141-3. doi: 10.1016/j.jconrel.2015.09.041.',
                                     'Kantor D, Panchal S, Patel V, Bucior I, Rauck R. Treatment of Postherpetic Neuralgia with Gastroretentive Gabapentin: Interaction of Patient Demographics, Disease Characteristics, and Efficacy Outcomes. J Pain. 2015 Sep 23. pii: S1526-5900(15)00868-8. doi: 10.1016/j.jpain.2015.08.011.',
                                     'DeLong MR, Wichmann T. Basal Ganglia Circuits as Targets for Neuromodulation in Parkinson Disease. JAMA Neurol. 2015 Sep 26:1-7. doi: 10.1001/jamaneurol.2015.2397.',
                                     'Levine YC, Matos J, Rosenberg MA, Manning WJ, Josephson ME, Buxton AE. Left Ventricular Sphericity Independently Predicts Appropriate ICD Therapy. Heart Rhythm. 2015 Sep 23. pii: S1547-5271(15)01192-3. doi: 10.1016/j.hrthm.2015.09.022.',
                                     'Quick-Weller J, Kann G, Lescher S, Imöhl L, Seifert V, Weise L, Brodt HR, Marquardt G. Impact of stereotactic biopsy in HIV patients. World Neurosurg. 2015 Sep 23. pii: S1878-8750(15)01200-0. doi: 10.1016/j.wneu.2015.09.037.',
                                     'Elliott P, Charron P, Blanes JR, Tavazzi L, Tendera M, Konté M, Laroche C, Maggioni AP; EORP Cardiomyopathy Registry Pilot Investigators. European Cardiomyopathy Pilot Registry: EURObservational Research Programme of the European Society of Cardiology. Eur Heart J. 2015 Sep 25. pii: ehv497.'
                        ]

    data = {'frequencies': [random() for i in xrange(70)],
            'topics':
                [
                    {
                        'terms': ['cells', 'cell', 'lymphocytes', 'culture', 'cultures', 'surface', 'normal', 'spleen'],
                        'articles': sample_articles_1
                    },
                    {
                        'terms': ['patients', 'disease', 'treatment', 'therapy', 'treated', 'months', 'patient', 'group'],
                        'articles': sample_articles_2
                    },
                    {
                        'terms': ['activity', 'enzyme', 'ph', 'enzymes', 'activities', 'dehydrogenase', 'substrate', 'purified'],
                        'articles': sample_articles_1
                    },
                    {
                        'terms': ['virus', 'infection', 'infected', 'strains', 'viral', 'viruses', 'mice', 'influenza'],
                        'articles': sample_articles_2
                    },
                    {
                        'terms': ['cases', 'case', 'diagnosis', 'clinical', 'disease', 'patient', 'reported', 'syndrome'],
                        'articles': sample_articles_1
                    }
                ]
    }

    return json.dumps(data)


if __name__ == "__main__":
    print dummy_query('test query')